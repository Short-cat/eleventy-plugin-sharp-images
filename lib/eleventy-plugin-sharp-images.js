// Required dependencies
const path = require("path");
const sharp = require("sharp");
const crypto = require("crypto");
const fs = require("fs").promises;
const os = require("os");

// Sharp methods that should be excluded from filter creation
const exclude = [
    "clone",
    "metadata",
    "stats",
    "trim", // trim is a built-in filter for most templating languages
    "toFile",
    "toBuffer",
];

const supportedFormats = ["webp", "avif", "jpeg", "jpg", "png", "tiff"];

const defaultOptions = {
    urlPath: "/assets/images",
    outputDir: "public/assets/images",
    cacheStrategy: "auto", // "auto" | "content" | "stats"
    concurrency: "auto",
};

// Cache file location
const CACHE_DIR = ".cache";
const CACHE_FILE_NAME = "eleventy-plugin-sharp-images.json";

// Add a helper to manage memory pressure
class MemoryManager {
    constructor(maxBufferSize = 100 * 1024 * 1024) {
        // 100MB default
        this.currentSize = 0;
        this.maxSize = maxBufferSize;
    }

    canAllocate(size) {
        return this.currentSize + size <= this.maxSize;
    }

    allocate(size) {
        this.currentSize += size;
    }

    free(size) {
        this.currentSize = Math.max(0, this.currentSize - size);
    }

    reset() {
        this.currentSize = 0;
    }
}

const memoryManager = new MemoryManager();

// Detect if we're in a CI/production environment
function isProductionBuild() {
    return process.env.NETLIFY === "true" || process.env.CI === "true" || process.env.NODE_ENV === "production";
}

// Get all Sharp methods to expose as Eleventy filters
const forwardMethods = Object.keys(sharp.prototype).filter((method) => {
    return !method.startsWith("_") && !exclude.includes(method);
});

// Track all cache keys used in this build (for pruning)
const activeCacheKeys = new Set();

// Simple in-memory cache for file stats and metadata to avoid repeated disk access
const fileStatsCache = new Map();
const STATS_CACHE_TTL = 5000;

const imageBufferCache = new Map();
const BUFFER_CACHE_TTL = 30000;

const buildResultCache = new Map();

// Helper function to get cached file stats
async function getCachedStats(filePath) {
    const cached = fileStatsCache.get(filePath);
    if (cached && Date.now() - cached.time < STATS_CACHE_TTL) {
        return cached.stats;
    }

    const stats = await fs.stat(filePath);
    fileStatsCache.set(filePath, { stats, time: Date.now() });
    return stats;
}

// Helper function to get cached image buffer
async function getCachedImageBuffer(filePath) {
    const cached = imageBufferCache.get(filePath);
    if (cached && Date.now() - cached.time < BUFFER_CACHE_TTL) {
        return cached.buffer;
    }

    const buffer = await fs.readFile(filePath);
    imageBufferCache.set(filePath, { buffer, time: Date.now() });
    return buffer;
}

// Async processing queue management with increased concurrency
class AsyncImageQueue {
    constructor(concurrency = os.cpus().length) {
        // Use all CPU cores
        this.concurrency = concurrency;
        this.running = 0;
        this.queue = [];
        this.results = new Map();
    }

    async add(task) {
        return new Promise((resolve, reject) => {
            this.queue.push({ task, resolve, reject });
            this.process();
        });
    }

    async process() {
        if (this.running >= this.concurrency || this.queue.length === 0) {
            return;
        }

        this.running++;
        const { task, resolve, reject } = this.queue.shift();

        try {
            const result = await task();
            resolve(result);
        } catch (error) {
            reject(error);
        } finally {
            this.running--;
            // Process next item in queue
            setImmediate(() => this.process());
        }
    }

    async waitForAll() {
        while (this.running > 0 || this.queue.length > 0) {
            await new Promise((resolve) => setImmediate(resolve));
        }
    }
}

// Persistent cache management
class PersistentCache {
    constructor(cacheFilePath) {
        this.cacheFilePath = cacheFilePath;
        this.memoryCache = new Map();
        this.isDirty = false;
    }

    async load() {
        try {
            const cacheData = await fs.readFile(this.cacheFilePath, "utf8");
            const parsed = JSON.parse(cacheData);

            // Validate cache version/format
            if (parsed.version === "1.0") {
                this.memoryCache = new Map(parsed.entries);
                console.log(`Sharp Plugin: Loaded ${this.memoryCache.size} cached entries`);
            } else {
                console.log("Sharp Plugin: Cache version mismatch, starting fresh");
                this.memoryCache = new Map();
            }
        } catch (error) {
            // Cache doesn't exist or is corrupted, start fresh
            this.memoryCache = new Map();
        }
    }

    async save() {
        if (!this.isDirty) return;

        try {
            // Ensure cache directory exists
            const cacheDir = path.dirname(this.cacheFilePath);
            await fs.mkdir(cacheDir, { recursive: true });

            const cacheData = {
                version: "1.0",
                timestamp: new Date().toISOString(),
                entries: [...this.memoryCache],
            };

            // Write to temporary file first (atomic write)
            const tempFile = `${this.cacheFilePath}.tmp`;
            await fs.writeFile(tempFile, JSON.stringify(cacheData, null, 2));
            await fs.rename(tempFile, this.cacheFilePath);

            console.log(`Sharp Plugin: Saved ${this.memoryCache.size} cache entries`);
        } catch (error) {
            console.error("Sharp Plugin: Failed to save cache:", error.message);
        }
    }

    get(key) {
        return this.memoryCache.get(key);
    }

    set(key, value) {
        this.memoryCache.set(key, value);
        this.isDirty = true;
    }

    has(key) {
        return this.memoryCache.has(key);
    }

    clear() {
        this.memoryCache.clear();
        this.isDirty = true;
    }

    // Prune old entries that no longer exist
    async prune(activeKeys) {
        const sizeBefore = this.memoryCache.size;
        for (const [key, value] of this.memoryCache) {
            if (!activeKeys.has(key)) {
                this.memoryCache.delete(key);
                this.isDirty = true;
            }
        }
        const removed = sizeBefore - this.memoryCache.size;
        if (removed > 0) {
            console.log(`Sharp Plugin: Pruned ${removed} stale cache entries`);
        }
    }
}

// Global declaration (near other globals)
let imageQueue;
let persistentCache;

// Optimized cache key generation that minimizes file I/O
async function generateCacheKey(inputPath, operations, strategy = "auto", retries = 3) {
    if (strategy === "auto") {
        strategy = isProductionBuild() ? "content" : "stats";
    }

    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            if (strategy === "stats") {
                // Fast mode: Use cached file stats (good for local dev)
                const stats = await getCachedStats(inputPath);
                const keyData = {
                    path: inputPath,
                    size: stats.size,
                    mtime: stats.mtime.getTime(),
                    operations: operations,
                    strategy: "stats",
                };
                return crypto.createHash("md5").update(JSON.stringify(keyData)).digest("hex");
            } else if (strategy === "content") {
                // Read file into buffer first, just like in processImageWithSharp
                const imageBuffer = await fs.readFile(inputPath);

                // Get metadata from buffer instead of file path
                const metadata = await sharp(imageBuffer).metadata();

                // Use metadata for cache key
                const keyData = {
                    width: metadata.width,
                    height: metadata.height,
                    format: metadata.format,
                    size: metadata.size,
                    density: metadata.density,
                    channels: metadata.channels,
                    hasAlpha: metadata.hasAlpha,
                    operations: operations,
                    strategy: "content",
                };
                return crypto.createHash("md5").update(JSON.stringify(keyData)).digest("hex");
            }
        } catch (error) {
            // If this is not the last attempt and the error suggests file is temporarily unavailable, wait and retry
            if (attempt < retries - 1 && (error.code === "ENOENT" || error.code === "EACCES" || error.code === "EBUSY")) {
                await new Promise((resolve) => setTimeout(resolve, 50 * (attempt + 1)));
                continue;
            }

            // If file doesn't exist or other persistent error, return a unique key that will cause processing to fail gracefully
            console.warn(`Sharp Plugin: Failed to generate cache key for ${inputPath}: ${error.message}`);
            return crypto
                .createHash("md5")
                .update(`${inputPath}:${Date.now()}:${JSON.stringify(operations)}:error`)
                .digest("hex");
        }
    }
}

// Helper function - generates an output URL based on content hash
function generateOutputUrl(config, options, contentHash) {
    const resolvedInputPath = path.posix.normalize(config.inputPath.replace(/\\/g, "/"));
    const inputExt = path.extname(resolvedInputPath);
    const inputBase = path.basename(resolvedInputPath, inputExt);

    // Determine output format from operations - add defensive check
    const formatOperation = Array.isArray(config.operations)
        ? config.operations.find((op) => {
              const method = op.method === "jpg" ? "jpeg" : op.method;
              return supportedFormats.includes(method);
          })
        : null;

    const outputExt = formatOperation ? `.${formatOperation.method}` : inputExt;
    const outputFilename = `${inputBase}-${contentHash.substring(0, 8)}${outputExt}`;

    return path.posix.join(options.urlPath, outputFilename);
}

// Optimized Sharp operation application - no longer async
function applySharpOperation(pipeline, operation) {
    const { method, args } = operation;

    // Special handling for resize operations to preserve transparency
    if (method === "resize") {
        const resizeOptions = args[0] || {};
        if (!resizeOptions.background) {
            resizeOptions.background = { r: 0, g: 0, b: 0, alpha: 0 };
        }
        return pipeline.resize(resizeOptions);
    }

    // Special handling for rotation to preserve transparency
    if (method === "rotate") {
        const angle = args[0] || 0;
        const rotateOptions = args[1] || {};
        if (!rotateOptions.background) {
            rotateOptions.background = { r: 0, g: 0, b: 0, alpha: 0 };
        }
        return pipeline.rotate(angle, rotateOptions);
    }

    // Format-specific optimization settings
    if (method === "webp") {
        const webpOptions = args[0] || {};
        return pipeline.webp({
            quality: webpOptions.quality || 80,
            effort: webpOptions.effort || 4,
            ...webpOptions,
        });
    }

    if (method === "avif") {
        const avifOptions = args[0] || {};
        return pipeline.avif({
            quality: avifOptions.quality || 75,
            effort: avifOptions.effort || 4,
            ...avifOptions,
        });
    }

    if (method === "png") {
        const pngOptions = args[0] || {};
        return pipeline.png({
            quality: pngOptions.quality || 90,
            compressionLevel: pngOptions.compressionLevel || 6,
            ...pngOptions,
        });
    }

    if (method === "jpeg" || method === "jpg") {
        const jpegOptions = args[0] || {};
        return pipeline.jpeg({
            quality: jpegOptions.quality || 85,
            progressive: jpegOptions.progressive !== false,
            ...jpegOptions,
        });
    }

    // For all other operations, apply them directly
    return pipeline[method](...args);
}

// Core image processing function with all optimizations
async function processImageWithSharp(config, options, retries = 3) {
    const cleanInputPath = path.join(options.inputDir, config.inputPath.replace(/^\/+/, ""));
    const resolvedInputPath = path.resolve(cleanInputPath);

    // Ensure source and output paths are different
    const outputDir = path.resolve(options.outputDir);
    if (resolvedInputPath.startsWith(outputDir)) {
        throw new Error(`Sharp Plugin: Source image cannot be in the output directory: ${resolvedInputPath}`);
    }

    // In development, we need to be extra careful about file access
    const isDev = !isProductionBuild();

    for (let attempt = 0; attempt < retries; attempt++) {
        try {
            // Generate cache key
            const cacheKey = await generateCacheKey(resolvedInputPath, config.operations, options.cacheStrategy);
            activeCacheKeys.add(cacheKey);

            // Check cache first
            if (persistentCache.has(cacheKey)) {
                const cachedUrl = persistentCache.get(cacheKey);
                const outputFilename = path.basename(cachedUrl);
                const outputFilePath = path.join(options.outputDir, outputFilename);

                try {
                    await fs.access(outputFilePath);
                    return cachedUrl;
                } catch {
                    console.log(`Sharp Plugin: Cached file missing, regenerating: ${outputFilename}`);
                }
            }

            const buildCacheKey = `${resolvedInputPath}:${cacheKey}`;
            if (buildResultCache.has(buildCacheKey)) {
                return buildResultCache.get(buildCacheKey);
            }

            // Generate output paths
            const outputUrl = generateOutputUrl(config, options, cacheKey);
            const outputFilename = path.basename(outputUrl);
            const outputFilePath = path.join(options.outputDir, outputFilename);

            // Ensure output directory exists
            await fs.mkdir(options.outputDir, { recursive: true });

            // CRITICAL: Read the entire file into memory before processing
            // This ensures we release the file handle immediately
            let imageBuffer;
            try {
                imageBuffer = await getCachedImageBuffer(resolvedInputPath);
            } catch (error) {
                if (isDev && attempt < retries - 1 && error.code === "EBUSY") {
                    // In dev, if file is busy, wait a bit and retry
                    await new Promise((resolve) => setTimeout(resolve, 100 * (attempt + 1)));
                    continue;
                }
                throw error;
            }

            // Create Sharp instance from buffer instead of file path
            // This completely decouples us from the file system
            let sharpPipeline = sharp(imageBuffer, {
                unlimited: true,
                failOn: "none",
                sequentialRead: false, // Don't need sequential read for buffers
            });

            // Apply all operations
            if (Array.isArray(config.operations)) {
                for (const operation of config.operations) {
                    sharpPipeline = applySharpOperation(sharpPipeline, operation);
                }
            }

            // Process and save with atomic write
            const tempOutputPath = `${outputFilePath}.tmp-${Date.now()}`;
            await sharpPipeline.toFile(tempOutputPath);

            // Atomic rename with retry for Windows
            let renameAttempts = 3;
            while (renameAttempts > 0) {
                try {
                    await fs.rename(tempOutputPath, outputFilePath);
                    break;
                } catch (error) {
                    if (error.code === "EBUSY" && renameAttempts > 1) {
                        await new Promise((resolve) => setTimeout(resolve, 50));
                        renameAttempts--;
                    } else {
                        // Clean up temp file if rename ultimately fails
                        try {
                            await fs.unlink(tempOutputPath);
                        } catch {}
                        throw error;
                    }
                }
            }

            // Update cache
            persistentCache.set(cacheKey, outputUrl);
            buildResultCache.set(buildCacheKey, outputUrl);
            return outputUrl;
        } catch (error) {
            // Enhanced retry logic for development
            const isFileAccessError = error.code === "ENOENT" || error.code === "EACCES" || error.code === "EBUSY" || error.code === "EPERM";

            if (attempt < retries - 1 && isFileAccessError) {
                const delay = isDev ? 200 * (attempt + 1) : 100 * (attempt + 1);
                console.log(`Sharp Plugin: File operation failed (${error.code}), retrying in ${delay}ms: ${resolvedInputPath}`);
                await new Promise((resolve) => setTimeout(resolve, delay));
                continue;
            }

            throw new Error(`Sharp Plugin: Failed to process image "${config.inputPath}" after ${attempt + 1} attempts: ${error.message}`);
        }
    }
}

// Creates the plugin with all optimizations
function createSharpPlugin(eleventyConfig, pluginOptions) {
    // Merge user options with defaults
    const options = Object.assign({}, defaultOptions, pluginOptions);
    options.inputDir = eleventyConfig.dir.input || ".";

    // Initialize the image queue with dynamic concurrency
    let concurrencyLevel;
    if (options.concurrency === "auto") {
        concurrencyLevel = isProductionBuild() ? os.cpus().length : Math.max(2, Math.floor(os.cpus().length / 2));
    } else {
        concurrencyLevel = options.concurrency;
    }

    imageQueue = new AsyncImageQueue(concurrencyLevel);

    // Initialize persistent cache
    const cacheFilePath = path.join(process.cwd(), CACHE_DIR, CACHE_FILE_NAME);
    persistentCache = new PersistentCache(cacheFilePath);

    // Log cache strategy and concurrency
    const effectiveStrategy = options.cacheStrategy === "auto" ? (isProductionBuild() ? "content" : "stats") : options.cacheStrategy;

    console.log(`Sharp Plugin: Using "${effectiveStrategy}" cache strategy with ${concurrencyLevel} concurrent workers`);

    // Validate configuration
    if (!options.outputDir) {
        throw new Error("Sharp Plugin: outputDir option is required");
    }

    // Load persistent cache on build start
    eleventyConfig.on("eleventy.before", async () => {
        await persistentCache.load();
        activeCacheKeys.clear();
        fileStatsCache.clear();
        imageBufferCache.clear();
        buildResultCache.clear();
    });

    // Save persistent cache and cleanup after build
    eleventyConfig.on("eleventy.after", async () => {
        await imageQueue.waitForAll();

        // Prune stale cache entries
        await persistentCache.prune(activeCacheKeys);

        // Save cache to disk
        await persistentCache.save();
    });

    // Create filters for each Sharp method
    forwardMethods.forEach((sharpMethodName) => {
        eleventyConfig.addFilter(sharpMethodName, function (sharpConfig, ...methodArguments) {
            // Handle the case where sharpConfig is undefined or null
            if (!sharpConfig) {
                throw new Error(`Sharp plugin: ${sharpMethodName} filter received undefined or null input. Make sure the image path variable exists and is not empty.`);
            }

            // Handle the case where this is the first filter in the chain
            if (typeof sharpConfig === "string") {
                sharpConfig = {
                    inputPath: sharpConfig,
                    operations: [],
                };
            }

            // Additional safety check
            if (!sharpConfig.inputPath) {
                throw new Error(`Sharp plugin: ${sharpMethodName} filter received a config object without inputPath. Config: ${JSON.stringify(sharpConfig)}`);
            }

            // Return new configuration object with added operation
            return {
                inputPath: sharpConfig.inputPath,
                operations: [
                    ...(Array.isArray(sharpConfig.operations) ? sharpConfig.operations : []),
                    {
                        method: sharpMethodName,
                        args: methodArguments,
                    },
                ],
                toString() {
                    return JSON.stringify(this);
                },
            };
        });
    });

    // Shortcode generates placeholders for later processing
    eleventyConfig.addShortcode("getUrl", (sharpConfig) => {
        let config;

        // Handle both string inputs and configuration objects
        if (typeof sharpConfig === "string") {
            config = { inputPath: sharpConfig, operations: [] };
        } else {
            config = sharpConfig;
        }

        // Validate input path
        if (!config.inputPath || config.inputPath.trim() === "") {
            throw new Error("Sharp plugin: inputPath cannot be empty");
        }

        const configJson = JSON.stringify(config);
        const encodedConfig = Buffer.from(configJson).toString("base64");

        // Generate a fallback URL for the placeholder
        const inputExt = path.extname(config.inputPath).toLowerCase();
        const formatOperation = config.operations?.find((op) => {
            const method = op.method === "jpg" ? "jpeg" : op.method;
            return supportedFormats.includes(method);
        });

        let outputExt = formatOperation ? `.${formatOperation.method}` : inputExt;
        if (!outputExt || outputExt === ".") {
            outputExt = ".jpg"; // Default fallback
        }

        const placeholderHash = crypto.createHash("md5").update(configJson).digest("hex");
        const tempUrl = path.posix.join(options.urlPath, `temp-${placeholderHash.substring(0, 8)}${outputExt}`);

        // Create placeholder comment that transform will process
        const placeholder = `<!-- SHARP_IMAGE ${encodedConfig} -->`;
        return `${placeholder}${tempUrl}`;
    });

    // Optimized single-pass transform for processing all images
    eleventyConfig.addTransform("sharpTransform", async (content, outputPath) => {
        // Only process HTML files - ensure outputPath is a string first
        if (typeof outputPath !== "string" || !outputPath.endsWith(".html")) return content;

        const configCommentRegex = /<!-- SHARP_IMAGE (.*?) -->([^\s<"]+)/g;
        const processPromises = [];
        const replacements = [];

        // Single pass to find and queue all images
        let match;
        while ((match = configCommentRegex.exec(content)) !== null) {
            const startIndex = match.index;
            const fullMatch = match[0];
            const fallbackUrl = match[2];

            try {
                const encodedConfig = match[1];
                const configJson = Buffer.from(encodedConfig, "base64").toString("utf8");
                const config = JSON.parse(configJson);

                const promise = imageQueue.add(async () => {
                    try {
                        const url = await processImageWithSharp(config, options);
                        return { startIndex, length: fullMatch.length, url };
                    } catch (error) {
                        console.error(`Sharp Plugin: Failed to process ${config.inputPath}:`, error.message);
                        return { startIndex, length: fullMatch.length, url: fallbackUrl };
                    }
                });

                processPromises.push(promise);
            } catch (error) {
                console.warn(`Sharp Plugin: Failed to parse configuration:`, error.message);
            }
        }

        // If no images found, return early
        if (processPromises.length === 0) {
            return content;
        }

        // Wait for all processing to complete
        const results = await Promise.all(processPromises);

        // Single replacement pass (from end to start to preserve indices)
        results.sort((a, b) => b.startIndex - a.startIndex);
        let finalContent = content;
        for (const result of results) {
            finalContent = finalContent.slice(0, result.startIndex) + result.url + finalContent.slice(result.startIndex + result.length);
        }

        return finalContent;
    });
}

module.exports = function (eleventyConfig, pluginOptions) {
    createSharpPlugin(eleventyConfig, pluginOptions);
};
