/**
 * @fileoverview SVG manipulation utilities for Discograph
 * This module provides functionality for SVG initialization, sizing, definition setup,
 * and SVG export capabilities. It handles SVG element manipulation, styling, and
 * conversion to other image formats.
 */

import * as d3 from "d3";
import { saveAs } from "file-saver";
import { dg } from "./dg";
import { showMessage, clearMessages } from "./init";

interface GradientStop {
    offset: string;
    color: string;
    opacity: string;
}

/**
 * Initializes the SVG element with basic setup
 * - Sets up window dimensions
 * - Creates SVG definitions (markers, gradients)
 * - Initializes tooltips for nodes and links
 */
export const initSvg = (): void => {
    // Setup window dimensions on SVG element
    setSvgSize();

    // Setup SVG common definitions
    setupSvgDefs();

    // No need to initialize tooltips here since we're using Bootstrap tooltips
    d3.select("#svg");
};

/**
 * Sets the size and viewport attributes of the main SVG element
 * Uses global dg.dimensions and dg.svg_dimensions for sizing
 */
export const setSvgSize = (): void => {
    const [width, height] = dg.dimensions;
    const [svgWidth, svgHeight] = dg.svg_dimensions;

    // Setup window dimensions on SVG element
    d3.select("#svg")
        .attr("width", String(width))
        .attr("height", String(height))
        .attr("viewBox", `0 0 ${svgWidth} ${svgHeight}`)
        .attr("preserveAspectRatio", "none");
};

/**
 * Resets the SVG element size by removing explicit width/height CSS properties
 */
export const resetSvgSize = (): void => {
    const svg = document.querySelector("#svg");
    if (svg instanceof SVGElement) {
        svg.style.cssText = "";
    }
};

/**
 * Sets up SVG definitions including:
 * - Arrowhead marker for directed connections
 * - Aggregate marker for relationship indicators
 * - Radial gradient for visual effects
 */
export const setupSvgDefs = (): void => {
    const defs = d3.select("#svg").append("defs");

    // ARROWHEAD
    defs.append("marker")
        .attr("id", "arrowhead")
        .attr("viewBox", "-5 -5 10 10")
        .attr("refX", 4)
        .attr("refY", 0)
        .attr("markerWidth", 5)
        .attr("markerHeight", 5)
        .attr("markerUnits", "strokeWidth")
        .attr("orient", "auto")
        .append("path")
        .attr("d", "M 0,0 m -5,-5 L 5,0 L -5,5 L -2.5,0 L -5,-5 Z")
        .attr("stroke-linecap", "round")
        .attr("stroke-linejoin", "round");

    // AGGREGATE
    defs.append("marker")
        .attr("id", "aggregate")
        .attr("viewBox", "-5 -5 10 10")
        .attr("refX", 5)
        .attr("refY", 0)
        .attr("markerWidth", 5)
        .attr("markerHeight", 5)
        .attr("markerUnits", "strokeWidth")
        .attr("orient", "auto")
        .append("path")
        .attr("d", "M 0,0 m 5,0 L 0,-3 L -5,0 L 0,3 L 5,0 Z")
        .attr("fill", "#fff")
        .attr("stroke", "#000")
        .attr("stroke-linecap", "round")
        .attr("stroke-linejoin", "round")
        .attr("stroke-width", 1.5);

    // RADIAL GRADIENT
    const gradient = defs
        .append("radialGradient")
        .attr("id", "radial-gradient");

    const gradientStops: GradientStop[] = [
        { offset: "0%", color: "#333", opacity: "1.0" },
        { offset: "50%", color: "#333", opacity: "0.333" },
        { offset: "75%", color: "#333", opacity: "0.111" },
        { offset: "100%", color: "#333", opacity: "0.0" },
    ];

    gradientStops.forEach(({ offset, color, opacity }) => {
        gradient
            .append("stop")
            .attr("offset", offset)
            .attr("stop-color", color)
            .attr("stop-opacity", opacity);
    });
};

/**
 * Exports the SVG as a PNG image file
 * @param width - The desired width of the output image
 * @param height - The desired height of the output image
 */
export const printSvg = (width: number, height: number): void => {
    showMessage("info", "Saving image to disk, please wait...");
    const svgNode = d3.select("#svg").node() as SVGElement | null;

    if (!svgNode) {
        throw new Error("SVG element not found");
    }

    const svgString = getSvgString(svgNode);
    svgString2Image(svgString, 2 * width, 2 * height, "png", saveBlob);
};

/**
 * Callback function to save the blob data as a file
 * @param dataBlob - The blob data to save
 * @param filesize - The size of the file
 */
function saveBlob(dataBlob: Blob, _filesize: number): void {
    const entityKey = dg.network.pageData.selectedNodeKey;
    const node = dg.network.data.nodeMap.get(entityKey);
    if (!node) {
        throw new Error("Selected node not found");
    }

    const filename = `Discograph - ${node.name}.png`;
    saveAs(dataBlob, filename);

    clearMessages(10);
    showMessage("success", "Saving image complete");
    clearMessages(10000);
}

/**
 * Converts an SVG node to a string representation
 * @param svgNode - The SVG DOM node to convert
 * @returns The serialized SVG string with proper namespace handling
 */
const getSvgString = (svgNode: SVGElement): string => {
    svgNode.setAttribute("xlink", "http://www.w3.org/1999/xlink");
    const cssStyleText = getCSSStyles(svgNode);
    appendCSS(cssStyleText, svgNode);

    const serializer = new XMLSerializer();
    let svgString = serializer.serializeToString(svgNode);
    svgString = svgString.replace(/(\w+)?:?xlink=/g, "xmlns:xlink="); // Fix root xlink without namespace
    svgString = svgString.replace(/NS\d+:href/g, "xlink:href"); // Safari NS namespace fix

    return svgString;
};

/**
 * Gets CSS styles for SVG elements
 * @param parentElement - The parent SVG element
 * @returns Concatenated CSS rules
 */
const getCSSStyles = (parentElement: SVGElement): string => {
    const selectorTextArr = new Set<string>();

    // Add Parent element Id and Classes
    if (parentElement.id) {
        selectorTextArr.add(`#${parentElement.id}`);
    }
    Array.from(parentElement.classList).forEach((className) => {
        selectorTextArr.add(`.${className}`);
    });

    // Process all child nodes
    const nodes = parentElement.getElementsByTagName("*");
    Array.from(nodes).forEach((node) => {
        if (node.id) {
            selectorTextArr.add(`#${node.id}`);
        }

        Array.from(node.classList).forEach((className) => {
            // Basic class
            selectorTextArr.add(`.${className}`);

            // Node type with class
            if (node.nodeName) {
                selectorTextArr.add(`${node.nodeName}.${className}`);
            }

            // Parent relationships
            const parentNode = node.parentNode;
            if (parentNode instanceof Element) {
                if (parentNode.id) {
                    selectorTextArr.add(`#${parentNode.id} .${className}`);
                    if (node.nodeName) {
                        selectorTextArr.add(
                            `#${parentNode.id} ${node.nodeName}.${className}`,
                        );
                    }
                }

                Array.from(parentNode.classList).forEach((parentClass) => {
                    selectorTextArr.add(`.${parentClass}`);
                    selectorTextArr.add(`.${parentClass} .${className}`);
                    if (node.nodeName) {
                        selectorTextArr.add(
                            `.${parentClass} ${node.nodeName}.${className}`,
                        );
                    }
                });

                // Grandparent relationships
                const grandParentNode = parentNode.parentNode;
                if (grandParentNode instanceof Element && grandParentNode.id) {
                    selectorTextArr.add(`#${grandParentNode.id} .${className}`);
                    Array.from(parentNode.classList).forEach((parentClass) => {
                        selectorTextArr.add(
                            `#${grandParentNode.id} .${parentClass}`,
                        );
                        selectorTextArr.add(
                            `#${grandParentNode.id} .${parentClass} .${className}`,
                        );
                        if (node.nodeName) {
                            selectorTextArr.add(
                                `#${grandParentNode.id} .${parentClass} ${node.nodeName}.${className}`,
                            );
                        }
                    });
                }
            }
        });
    });

    // Extract CSS Rules
    let extractedCSSText = "";
    for (const sheet of document.styleSheets) {
        try {
            const cssRules = sheet.cssRules;
            if (!cssRules) continue;

            for (const rule of cssRules) {
                if (
                    rule instanceof CSSStyleRule &&
                    selectorTextArr.has(rule.selectorText)
                ) {
                    extractedCSSText += rule.cssText + "\n";
                }
            }
        } catch (e) {
            if (e instanceof Error && e.name !== "SecurityError") throw e; // for Firefox
            continue;
        }
    }

    return extractedCSSText;
};

/**
 * Appends CSS styles to an SVG element
 * @param cssText - The CSS text to append
 * @param element - The target SVG element
 */
const appendCSS = (cssText: string, element: SVGElement): void => {
    const styleElement = document.createElement("style");
    styleElement.setAttribute("type", "text/css");
    styleElement.textContent = cssText;
    const refNode = element.querySelector("defs") || element.firstChild;
    element.insertBefore(styleElement, refNode);
};

/**
 * Converts an SVG string to an image
 * @param svgString - The SVG string to convert
 * @param width - The desired width of the output image
 * @param height - The desired height of the output image
 * @param format - The output format (default: 'png')
 * @param callback - Callback function to handle the converted image
 */
const svgString2Image = (
    svgString: string,
    width: number,
    height: number,
    format: string = "png",
    callback: (blob: Blob, filesize: number) => void,
): void => {
    const imgsrc =
        "data:image/svg+xml;base64," +
        btoa(unescape(encodeURIComponent(svgString)));

    const canvas = document.createElement("canvas");
    const context = canvas.getContext("2d");

    if (!context) {
        throw new Error("Could not get canvas context");
    }

    canvas.width = width;
    canvas.height = height;

    const image = new Image();
    image.onload = function () {
        context.clearRect(0, 0, width, height);
        context.drawImage(image, 0, 0, width, height);

        canvas.toBlob((blob) => {
            if (!blob) {
                throw new Error("Failed to create blob from canvas");
            }
            callback(blob, blob.size);
        }, "image/" + format);
    };
    image.src = imgsrc;
};
