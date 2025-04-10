import type { SimNode, SimLink } from "./network/data";
import { clamp } from "./utils";

/**
 * Determines the color class for a node based on its type.
 * @param d - The node data object
 * @returns CSS class name for the node's color
 */
export const getNodeColorClass = (d: SimNode): string => {
    return d.type === "artist"
        ? getArtistNodeColorClass(d)
        : getLabelNodeColorClass(d);
};

/**
 * Determines the color class for an artist node based on its distance.
 * @param d - The artist node data object
 * @returns CSS class name in the format 'color-X' where X is a number from 0-8
 */
const getArtistNodeColorClass = (d: SimNode): string => {
    const index = clamp(d.distance, 0, 8) + 1;
    const clampedIndex = clamp(index, 0, 8);
    return `color-${clampedIndex}`;
};

/**
 * Determines the color class for a label node based on its distance.
 * @param d - The label node data object
 * @returns CSS class name in the format 'color-X' where X is a number from 0-8
 */
const getLabelNodeColorClass = (d: SimNode): string => {
    const index = clamp(d.distance, 0, 8) + 2;
    const clampedIndex = clamp(index, 0, 8);
    return `color-${clampedIndex}`;
};

/**
 * Determines the color class for a link between nodes based on the minimum distance
 * of its source and target nodes.
 * @param d - The link data object
 * @returns CSS class name in the format 'color-X' where X is a number from 0-8
 */
export const getLinkColorClass = (d: SimLink): string => {
    let distance = Math.min(d.source.distance, d.target.distance);
    distance = distance === 0 ? 2 : 5;
    const index = clamp(distance, 0, 8);
    return `color-${index}`;
};
