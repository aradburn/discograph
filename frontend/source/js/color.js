/**
 * Clamps a number between min and max values.
 * @param {number} num - The number to clamp
 * @param {number} min - The minimum value
 * @param {number} max - The maximum value
 * @returns {number} The clamped value
 */
export const clamp = (num, min, max) => Math.min(Math.max(num, min), max);

/**
 * Determines the color class for a node based on its type.
 * @param {Object} d - The node data object
 * @returns {string} CSS class name for the node's color
 */
export const getNodeColorClass = (d) => {
    return d.type === 'artist' 
        ? getArtistNodeColorClass(d) 
        : getLabelNodeColorClass(d);
};

/**
 * Determines the color class for an artist node based on its distance.
 * @param {Object} d - The artist node data object
 * @param {number} d.distance - The distance value of the node
 * @returns {string} CSS class name in the format 'color-X' where X is a number from 0-8
 */
const getArtistNodeColorClass = (d) => {
    const index = clamp(d.distance + 1, 0, 8);
    return `color-${index}`;
};

/**
 * Determines the color class for a label node based on its distance.
 * @param {Object} d - The label node data object
 * @param {number} d.distance - The distance value of the node
 * @returns {string} CSS class name in the format 'color-X' where X is a number from 0-8
 */
const getLabelNodeColorClass = (d) => {
    const index = clamp(d.distance + 2, 0, 8);
    return `color-${index}`;
};

/**
 * Determines the color class for a link between nodes based on the minimum distance
 * of its source and target nodes.
 * @param {Object} d - The link data object
 * @param {Object} d.source - The source node
 * @param {Object} d.target - The target node
 * @returns {string} CSS class name in the format 'color-X' where X is a number from 0-8
 */
export const getLinkColorClass = (d) => {
    let distance = Math.min(d.source.distance, d.target.distance);
    distance = distance === 0 ? 2 : 5;
    const index = clamp(distance, 0, 8);
    return `color-${index}`;
};
