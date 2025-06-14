// Required dependencies
const path = require("path");
const sharp = require("sharp");
const crypto = require("crypto");
const fs = require("fs").promises;

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

// Cache for tracking file states to avoid repeated file system checks
const fileStateCache = new Map();

// Helper function - generates an output URL based on content hash
function generateExpectedUrl(config, options, contentHash) {
    const resolvedInputPath = path.posix.normalize(config.inputPath.replace(/\\/g, "/"));
    const inputExt = path.extname(resolvedInputPath);
    const inputBase = path.basename(resolvedInputPath, inputExt);

    // Determine output format from operations
    const formatOperation = config.operations.find((op) => {
        const method = op.method === "jpg" ? "jpeg" : op.method;
        return supportedFormats.includes(method);
    });

    const outputExt = formatOperation ? `.${formatOperation.method}` : inputExt;

    // Create filename with content hash for cache busting
    const outputFilename = `${inputBase}-${contentHash.substring(0, 8)}${outputExt}`;

    return path.posix.join(options.urlPath, outputFilename);
}

// The next two helper functions work together to handle cases where you overwrite a file that already exists. In previous versions of this plugin, the image wouldn't update.
// To make the plugin work during this situation, we have to work around the fact that, when you overwrite a file, behind-the-scenes, it's just being deleted and resaved with the same name.
// Hence the reason for these functions. The plugin will wait for a file to be resaved before continuing with optimisations.

async function checkFileExists(filepath) {
    try {
        const stats = await fs.stat(filepath);
        return stats.isFile() && stats.size > 0;
    } catch (error) {
        return false;
    }
}

async function waitForFile(filepath, options = {}) {
    const maxAttempts = 10;
    const delay = 100;
    let lastError = null;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
        try {
            // First, check if file exists
            const exists = await checkFileExists(filepath);
            if (!exists) {
                lastError = new Error(`${filepath} does not exist or is empty`);
                await new Promise((resolve) => setTimeout(resolve, delay));
                continue;
            }

            // Try to open the file for reading to ensure it's not locked
            const fd = await fs.open(filepath, "r");

            try {
                const stats = await fd.stat();

                if (stats.size === 0) {
                    lastError = new Error(`File exists but has zero size`);
                    await fd.close();
                    await new Promise((resolve) => setTimeout(resolve, delay));
                    continue;
                }

                // Try to read first few bytes to ensure file is readable
                const buffer = Buffer.alloc(Math.min(512, stats.size));
                const { bytesRead } = await fd.read(buffer, 0, buffer.length, 0);

                if (bytesRead > 0) {
                    await fd.close();
                    return true;
                }
            } finally {
                try {
                    await fd.close();
                } catch (e) {}
            }
        } catch (error) {
            lastError = error;
            if (attempt < maxAttempts - 1) {
                await new Promise((resolve) => setTimeout(resolve, delay));
            }
        }
    }

    console.error(`Sharp Plugin: File not available after ${maxAttempts} attempts to access:`, lastError?.message);
    return false;
}

// Generates a cache key based on source image content and operations
async function generateContentBasedCacheKey(inputPath, operations, options) {
    const cacheKey = `${inputPath}:${JSON.stringify(operations)}`;

    // Check if we have a recent cache entry
    const cachedState = fileStateCache.get(cacheKey);
    if (cachedState && Date.now() - cachedState.timestamp < 1000) {
        return cachedState.hash;
    }

    try {
        // Wait for file to be fully written before reading
        const fileReady = await waitForFile(inputPath);
        if (!fileReady) {
            throw new Error(`File not available: ${inputPath}`);
        }

        // Read the source image file to include its content in the cache key
        const imageBuffer = await fs.readFile(inputPath);

        // Get file stats to include modification time
        const stats = await fs.stat(inputPath);

        // Create a hash from image content + modification time + operations
        const contentHash = crypto
            .createHash("md5")
            .update(imageBuffer)
            .update(stats.mtime.toISOString())
            .update(stats.size.toString())
            .update(JSON.stringify(operations))
            .digest("hex");

        // Cache the result
        fileStateCache.set(cacheKey, {
            hash: contentHash,
            timestamp: Date.now(),
        });

        return contentHash;
    } catch (error) {
        // If we can't read the file, generate a unique hash that will force reprocessing later
        console.warn(`Sharp Plugin: Could not generate content-based cache key for ${inputPath}:`, error.message);
        const fallbackHash = crypto.createHash("md5").update(inputPath).update(Date.now().toString()).update(JSON.stringify(operations)).digest("hex");
        return fallbackHash;
    }
}

// Get all Sharp methods to expose as Eleventy filters
const forwardMethods = Object.keys(sharp.prototype).filter((method) => {
    return !method.startsWith("_") && !exclude.includes(method);
});

// Applies a single Sharp operation with special handling for transparency preservation
async function applySharpOperation(pipeline, operation) {
    const { method, args } = operation;

    // Special handling for resize operations to preserve transparency
    if (method === "resize") {
        const resizeOptions = args[0] || {};

        // If no background is specified, use transparent background
        if (!resizeOptions.background) {
            resizeOptions.background = { r: 0, g: 0, b: 0, alpha: 0 };
        }

        return pipeline.resize(resizeOptions);
    }

    // Special handling for rotation to preserve transparency
    if (method === "rotate") {
        const angle = args[0] || 0;
        const rotateOptions = args[1] || {};

        // If no background is specified, use transparent background
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

// Core image processing function
async function processImageWithSharp(config, options, retryCount = 0) {
    const maxRetries = 3;

    // Clean and resolve the input path
    const cleanInputPath = path.join(options.inputDir, config.inputPath.replace(/^\/+/, ""));
    const resolvedInputPath = path.resolve(cleanInputPath);

    // Ensure source and output paths are different
    const outputDir = path.resolve(options.outputDir);
    if (resolvedInputPath.startsWith(outputDir)) {
        throw new Error(`Sharp Plugin: Source image cannot be in the output directory: ${resolvedInputPath}`);
    }

    try {
        // Wait for file to be ready before processing
        const fileReady = await waitForFile(resolvedInputPath);
        if (!fileReady) {
            if (retryCount < maxRetries) {
                await new Promise((resolve) => setTimeout(resolve, 1000));
                return processImageWithSharp(config, options, retryCount + 1);
            } else {
                throw new Error(`File not accessible after ${maxRetries} retries: ${resolvedInputPath}`);
            }
        }

        // Generate content-based cache key
        const contentBasedHash = await generateContentBasedCacheKey(resolvedInputPath, config.operations, options);

        // Generate expected output URL and file paths
        const expectedUrl = generateExpectedUrl(config, options, contentBasedHash);
        const outputFilename = path.basename(expectedUrl);
        const outputFilePath = path.join(options.outputDir, outputFilename);

        // Check if we already have this exact version processed
        try {
            const stats = await fs.stat(outputFilePath);
            if (stats.size > 0) {
                return expectedUrl;
            }
        } catch (error) {
            // File doesn't exist, so we need to process it
        }

        // Ensure the output directory exists
        await fs.mkdir(options.outputDir, { recursive: true });

        // Read the file into a buffer first to avoid file system race conditions
        const sourceBuffer = await fs.readFile(resolvedInputPath);

        // Create Sharp pipeline from buffer instead of file path
        let sharpPipeline = sharp(sourceBuffer, {
            unlimited: true,
            sequentialRead: false, // We're using a buffer, so sequential read isn't needed
            density: 72,
            failOn: "none", // Don't fail on warnings
        });

        // Apply each operation from the configuration
        for (const operation of config.operations) {
            sharpPipeline = await applySharpOperation(sharpPipeline, operation);
        }

        // Generate a temporary filename for atomic writes
        const tempFilePath = outputFilePath + ".tmp." + Date.now();

        // Write to temporary file first
        await sharpPipeline.toFile(tempFilePath);

        // Atomically rename the temp file to the final location
        try {
            await fs.rename(tempFilePath, outputFilePath);
        } catch (error) {
            try {
                await fs.copyFile(tempFilePath, outputFilePath);
                await fs.unlink(tempFilePath);
            } catch (copyError) {
                try {
                    await fs.unlink(tempFilePath);
                } catch (e) {}
                throw copyError;
            }
        }

        return expectedUrl;
    } catch (error) {
        if (retryCount < maxRetries && error.message.includes("ENOENT")) {
            await new Promise((resolve) => setTimeout(resolve, 1000));
            return processImageWithSharp(config, options, retryCount + 1);
        }
        throw error;
    }
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

    // Clear file state cache on each build
    eleventyConfig.on("eleventy.before", async () => {
        fileStateCache.clear();
    });

    // Create filters for each Sharp method
    forwardMethods.forEach((sharpMethodName) => {
        eleventyConfig.addFilter(sharpMethodName, function (sharpConfig, ...methodArguments) {
            // Handle the case where this is the first filter in the chain
            if (typeof sharpConfig === "string") {
                sharpConfig = {
                    inputPath: sharpConfig,
                    operations: [],
                };
            }

            // Return new configuration object with added operation
            return {
                inputPath: sharpConfig.inputPath,
                operations: [
                    ...sharpConfig.operations,
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

        // Encode configuration to prevent template engine parsing issues
        const encodedConfig = Buffer.from(configJson).toString("base64");

        // Generate a proper fallback URL
        const inputExt = path.extname(config.inputPath).toLowerCase();
        const formatOperation = config.operations?.find((op) => {
            const method = op.method === "jpg" ? "jpeg" : op.method;
            return supportedFormats.includes(method);
        });

        // Ensure we have a valid extension
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
        // Only process HTML files
        if (!outputPath?.endsWith(".html")) return content;

        const configCommentRegex = /<!-- SHARP_IMAGE (.*?) -->([^\s<"]+)/g;
        let match;

        const imageConfigurations = [];
        const processedImages = new Map();
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
            } catch (error) {}
        }

        // If no images found, return early
        if (imageConfigurations.length === 0) {
            return content;
        }

        // Process each image configuration
        for (const imageConfig of imageConfigurations) {
            try {
                const processedUrl = await processImageWithSharp(imageConfig.config, options);
                processedImages.set(imageConfig.placeholder, processedUrl);
            } catch (error) {
                // If processing fails, use the fallback URL so the build doesn't break
                console.error(`Sharp Plugin: Failed to process image "${imageConfig.config.inputPath}":`, error.message);
                processedImages.set(imageConfig.placeholder, imageConfig.fallbackUrl);
            }
        }

        // Replace all placeholders with their corresponding processed URLs
        for (const [placeholder, processedUrl] of processedImages) {
            finalContent = finalContent.replaceAll(placeholder, processedUrl);
        }

        return finalContent;
    });
}

module.exports = function (eleventyConfig, pluginOptions) {
    createSharpPlugin(eleventyConfig, pluginOptions);
};
