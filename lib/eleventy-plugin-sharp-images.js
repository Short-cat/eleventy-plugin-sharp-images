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
};

// Simplified cache for tracking processed images
const processedImages = new Map();

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

// Global queue instance
const imageQueue = new AsyncImageQueue();

// Helper function to generate cache key based on file stats + operations
async function generateCacheKey(inputPath, operations) {
    try {
        const stats = await fs.stat(inputPath);
        // Use file path, size, mtime and operations for cache key
        const keyData = {
            path: inputPath,
            size: stats.size,
            mtime: stats.mtime.getTime(),
            operations: operations,
        };
        return crypto.createHash("md5").update(JSON.stringify(keyData)).digest("hex");
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

// Get all Sharp methods to expose as Eleventy filters
const forwardMethods = Object.keys(sharp.prototype).filter((method) => {
    return !method.startsWith("_") && !exclude.includes(method);
});

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

// Core image processing function - now with async queue support
async function processImageWithSharp(config, options) {
    // Clean and resolve the input path
    const cleanInputPath = path.join(options.inputDir, config.inputPath.replace(/^\/+/, ""));
    const resolvedInputPath = path.resolve(cleanInputPath);

    // Ensure source and output paths are different
    const outputDir = path.resolve(options.outputDir);
    if (resolvedInputPath.startsWith(outputDir)) {
        throw new Error(`Sharp Plugin: Source image cannot be in the output directory: ${resolvedInputPath}`);
    }

    // Generate cache key based on file stats and operations
    const cacheKey = await generateCacheKey(resolvedInputPath, config.operations);

    // Check if we've already processed this exact combination
    if (processedImages.has(cacheKey)) {
        return processedImages.get(cacheKey);
    }

    // Generate output URL and file paths
    const outputUrl = generateOutputUrl(config, options, cacheKey);
    const outputFilename = path.basename(outputUrl);
    const outputFilePath = path.join(options.outputDir, outputFilename);

    // Check if output file already exists and is newer than source
    try {
        const [sourceStats, outputStats] = await Promise.all([fs.stat(resolvedInputPath), fs.stat(outputFilePath)]);

        if (outputStats.size > 0 && outputStats.mtime >= sourceStats.mtime) {
            // File exists and is up to date, cache and return
            processedImages.set(cacheKey, outputUrl);
            return outputUrl;
        }
    } catch (error) {
        // Output file doesn't exist or other error, continue with processing
    }

    // Ensure the output directory exists
    await fs.mkdir(options.outputDir, { recursive: true });

    // Create Sharp pipeline directly from file path
    let sharpPipeline = sharp(resolvedInputPath, {
        unlimited: true,
        failOn: "none", // Don't fail on warnings
    });

    // Apply each operation from the configuration - add defensive check
    if (Array.isArray(config.operations)) {
        for (const operation of config.operations) {
            sharpPipeline = await applySharpOperation(sharpPipeline, operation);
        }
    }

    // Process and save the image directly
    await sharpPipeline.toFile(outputFilePath);

    // Cache the result
    processedImages.set(cacheKey, outputUrl);

    return outputUrl;
}

// Creates the plugin
function createSharpPlugin(eleventyConfig, pluginOptions) {
    // Merge user options with defaults
    const options = Object.assign({}, defaultOptions, pluginOptions);
    options.inputDir = eleventyConfig.dir.input || ".";

    // Validate configuration
    if (!options.outputDir) {
        throw new Error("Sharp Plugin: outputDir option is required");
    }

    // Clear processed images cache on each build
    eleventyConfig.on("eleventy.before", async () => {
        processedImages.clear();
    });

    // Ensure all queued image processing completes before build finishes
    eleventyConfig.on("eleventy.after", async () => {
        await imageQueue.waitForAll();
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
