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
};

// Cache file location
const CACHE_DIR = ".cache";
const CACHE_FILE_NAME = "eleventy-plugin-sharp-images.json";

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

// Async processing queue management
class AsyncImageQueue {
    constructor(concurrency = Math.max(1, Math.floor(os.cpus().length / 2))) {
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

// Global instances
const imageQueue = new AsyncImageQueue();
let persistentCache;

// Helper function to generate cache key based on strategy
async function generateCacheKey(inputPath, operations, strategy = "auto") {
    try {
        // Auto-detect the best strategy
        if (strategy === "auto") {
            strategy = isProductionBuild() ? "content" : "stats";
        }

        if (strategy === "stats") {
            // Fast mode: Use file stats only (good for local dev)
            const stats = await fs.stat(inputPath);
            const keyData = {
                path: inputPath,
                size: stats.size,
                mtime: stats.mtime.getTime(),
                operations: operations,
                strategy: "stats",
            };
            return crypto.createHash("md5").update(JSON.stringify(keyData)).digest("hex");
        } else if (strategy === "content") {
            // Accurate mode: Use actual file content (good for production)
            const [stats, buffer] = await Promise.all([fs.stat(inputPath), fs.readFile(inputPath)]);

            // Create hash from file content
            const contentHash = crypto.createHash("md5").update(buffer).digest("hex");

            const keyData = {
                contentHash,
                size: stats.size, // Still include size for quick preliminary checks
                operations: operations,
                strategy: "content",
            };
            return crypto.createHash("md5").update(JSON.stringify(keyData)).digest("hex");
        }
    } catch (error) {
        // If file doesn't exist, return a unique key that will cause processing to fail gracefully
        return crypto
            .createHash("md5")
            .update(`${inputPath}:${Date.now()}:${JSON.stringify(operations)}`)
            .digest("hex");
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

// Applies a single Sharp operation with optimized settings
async function applySharpOperation(pipeline, operation) {
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

// Core image processing function - updated to use persistent cache
async function processImageWithSharp(config, options) {
    // Clean and resolve the input path
    const cleanInputPath = path.join(options.inputDir, config.inputPath.replace(/^\/+/, ""));
    const resolvedInputPath = path.resolve(cleanInputPath);

    // Ensure source and output paths are different
    const outputDir = path.resolve(options.outputDir);
    if (resolvedInputPath.startsWith(outputDir)) {
        throw new Error(`Sharp Plugin: Source image cannot be in the output directory: ${resolvedInputPath}`);
    }

    // Generate cache key based on file stats/content and operations
    const cacheKey = await generateCacheKey(resolvedInputPath, config.operations, options.cacheStrategy);

    // Track this cache key as active
    activeCacheKeys.add(cacheKey);

    // Check persistent cache first
    if (persistentCache.has(cacheKey)) {
        const cachedUrl = persistentCache.get(cacheKey);

        // Verify the output file still exists
        const outputFilename = path.basename(cachedUrl);
        const outputFilePath = path.join(options.outputDir, outputFilename);

        try {
            await fs.access(outputFilePath);
            return cachedUrl; // Cache hit!
        } catch {
            // Output file was deleted, need to regenerate
            console.log(`Sharp Plugin: Cached file missing, regenerating: ${outputFilename}`);
        }
    }

    // Generate output URL and file paths
    const outputUrl = generateOutputUrl(config, options, cacheKey);
    const outputFilename = path.basename(outputUrl);
    const outputFilePath = path.join(options.outputDir, outputFilename);

    // Ensure the output directory exists
    await fs.mkdir(options.outputDir, { recursive: true });

    // Create Sharp pipeline directly from file path
    let sharpPipeline = sharp(resolvedInputPath, {
        unlimited: true,
        failOn: "none", // Don't fail on warnings
    });

    // Apply each operation from the configuration
    if (Array.isArray(config.operations)) {
        for (const operation of config.operations) {
            sharpPipeline = await applySharpOperation(sharpPipeline, operation);
        }
    }

    // Process and save the image
    await sharpPipeline.toFile(outputFilePath);

    // Update persistent cache
    persistentCache.set(cacheKey, outputUrl);

    return outputUrl;
}

// Creates the plugin - updated with persistent cache support
function createSharpPlugin(eleventyConfig, pluginOptions) {
    // Merge user options with defaults
    const options = Object.assign({}, defaultOptions, pluginOptions);
    options.inputDir = eleventyConfig.dir.input || ".";

    // Initialize persistent cache with new path
    const cacheFilePath = path.join(process.cwd(), CACHE_DIR, CACHE_FILE_NAME);
    persistentCache = new PersistentCache(cacheFilePath);

    // Log cache strategy
    const effectiveStrategy = options.cacheStrategy === "auto" ? (isProductionBuild() ? "content" : "stats") : options.cacheStrategy;

    console.log(`Sharp Plugin: Using "${effectiveStrategy}" cache strategy`);

    // Validate configuration
    if (!options.outputDir) {
        throw new Error("Sharp Plugin: outputDir option is required");
    }

    // Load persistent cache on build start
    eleventyConfig.on("eleventy.before", async () => {
        await persistentCache.load();
        activeCacheKeys.clear();
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

    // Process all image placeholders after the template is rendered
    eleventyConfig.addTransform("sharpTransform", async (content, outputPath) => {
        // Only process HTML files - ensure outputPath is a string first
        if (typeof outputPath !== "string" || !outputPath.endsWith(".html")) return content;

        const configCommentRegex = /<!-- SHARP_IMAGE (.*?) -->([^\s<"]+)/g;
        let match;
        const imageConfigurations = [];
        let finalContent = content;

        // Extract all image configurations from the HTML
        while ((match = configCommentRegex.exec(content)) !== null) {
            try {
                const encodedConfig = match[1];
                const configJson = Buffer.from(encodedConfig, "base64").toString("utf8");
                const config = JSON.parse(configJson);

                imageConfigurations.push({
                    config: config,
                    placeholder: match[0],
                    fallbackUrl: match[2],
                });
            } catch (error) {
                console.warn(`Sharp Plugin: Failed to parse image configuration:`, error.message);
            }
        }

        // If no images found, return early
        if (imageConfigurations.length === 0) {
            return content;
        }

        // Process all images using the async queue for better concurrency control
        const processPromises = imageConfigurations.map((imageConfig) => {
            return imageQueue.add(async () => {
                try {
                    const processedUrl = await processImageWithSharp(imageConfig.config, options);
                    return { placeholder: imageConfig.placeholder, url: processedUrl };
                } catch (error) {
                    console.error(`Sharp Plugin: Failed to process image "${imageConfig.config.inputPath}":`, error.message);
                    return { placeholder: imageConfig.placeholder, url: imageConfig.fallbackUrl };
                }
            });
        });

        // Wait for all images to be processed through the queue
        const results = await Promise.all(processPromises);

        // Replace all placeholders with their corresponding processed URLs
        for (const result of results) {
            finalContent = finalContent.replaceAll(result.placeholder, result.url);
        }

        return finalContent;
    });
}

module.exports = function (eleventyConfig, pluginOptions) {
    createSharpPlugin(eleventyConfig, pluginOptions);
};
