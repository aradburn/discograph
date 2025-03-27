/**
 * Interfaces for node data structures
 */
interface BaseNode {
  type: "artist" | "label";
  distance: number;
}

interface ArtistNode extends BaseNode {
  type: "artist";
}

interface LabelNode extends BaseNode {
  type: "label";
}

type Node = ArtistNode | LabelNode;

interface Link {
  source: Node;
  target: Node;
}

/**
 * Clamps a number between min and max values.
 * @param num - The number to clamp
 * @param min - The minimum value
 * @param max - The maximum value
 * @returns The clamped value
 */
export const clamp = (num: number, min: number, max: number): number =>
  Math.min(Math.max(num, min), max);

/**
 * Determines the color class for a node based on its type.
 * @param d - The node data object
 * @returns CSS class name for the node's color
 */
export const getNodeColorClass = (d: Node): string => {
  return d.type === "artist"
    ? getArtistNodeColorClass(d)
    : getLabelNodeColorClass(d);
};

/**
 * Determines the color class for an artist node based on its distance.
 * @param d - The artist node data object
 * @returns CSS class name in the format 'color-X' where X is a number from 0-8
 */
const getArtistNodeColorClass = (d: ArtistNode): string => {
  const index = clamp(d.distance + 1, 0, 8);
  return `color-${index}`;
};

/**
 * Determines the color class for a label node based on its distance.
 * @param d - The label node data object
 * @returns CSS class name in the format 'color-X' where X is a number from 0-8
 */
const getLabelNodeColorClass = (d: LabelNode): string => {
  const index = clamp(d.distance + 2, 0, 8);
  return `color-${index}`;
};

/**
 * Determines the color class for a link between nodes based on the minimum distance
 * of its source and target nodes.
 * @param d - The link data object
 * @returns CSS class name in the format 'color-X' where X is a number from 0-8
 */
export const getLinkColorClass = (d: Link): string => {
  let distance = Math.min(d.source.distance, d.target.distance);
  distance = distance === 0 ? 2 : 5;
  const index = clamp(distance, 0, 8);
  return `color-${index}`;
};
