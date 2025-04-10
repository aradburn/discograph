/**
 * Network Graph Event Handlers
 * This file contains event handlers and control functions for a force-directed network graph,
 * implemented using D3.js. It manages node dragging behavior and layout controls.
 */

import { dg } from "../dg";
import { nodeTooltip } from "./tooltips";
import { onTick } from "./tick";
import { restartForceLayout, stopForceLayout } from "./forceLayout";
import type * as d3 from "d3";
import type { SimNode } from "./data";

interface D3DragEventWithSource<GElement extends Element, Datum, Subject>
    extends d3.D3DragEvent<GElement, Datum, Subject> {
    sourceEvent: MouseEvent | TouchEvent;
}

/**
 * Reheat the simulation when drag starts, and fix the subject position.
 * @param {D3DragEventWithSource<SVGGElement, SimNode, SimNode>} event - The drag event object
 */
export const onDragStart = (
    event: D3DragEventWithSource<SVGGElement, SimNode, SimNode>,
): void => {
    const node = event.subject;
    node.fx = node.x;
    node.fy = node.y;
    node.dragx = node.x;
    node.dragy = node.y;
    if (event.sourceEvent.type === "mousedown") {
        nodeTooltip.hide();
    }
};

/**
 * Updates node position during drag operation
 * @param {d3.D3DragEvent<SVGGElement, DraggableNode, DraggableNode>} event - The drag event object
 */
export const onDrag = (
    event: d3.D3DragEvent<SVGGElement, SimNode, SimNode>,
): void => {
    const node = event.subject;
    node.fx = event.x;
    node.fy = event.y;
    if (node.dragx !== node.x || node.dragy !== node.y) {
        node.dragx = node.x;
        node.dragy = node.y;
        if (!event.active) {
            restartForceLayout(0.3);
        }
    }
};

/**
 * Handles the end of a node drag operation
 * Releases the fixed position of the node and allows the simulation to continue
 * @param {D3DragEventWithSource<SVGGElement, DraggableNode, DraggableNode>} event - The drag event object
 */
export const onDragEnd = (
    event: D3DragEventWithSource<SVGGElement, SimNode, SimNode>,
): void => {
    const node = event.subject;
    if (node.dragx === node.x && node.dragy === node.y) {
        return;
    }
    if (!event.active) stopForceLayout();
    node.fx = null;
    node.fy = null;
    if (event.sourceEvent.type === "mouseup") {
        nodeTooltip.hide();
    }
};

/**
 * Initiates the network layout simulation
 * Shows the running indicator and enables interaction with nodes and links
 */
export const onNetworkStart = (): void => {
    dg.network.isRunningLayout = true;
    dg.network.tick = 0;

    dg.network.layers.link?.selectAll(".link").classed("noninteractive", false);
    dg.network.layers.node?.selectAll(".node").classed("noninteractive", false);
};

/**
 * Handles the completion of network layout simulation
 * Hides the running indicator and ensures nodes and links remain interactive
 * @param {d3.Simulation<DraggableNode, undefined>} event - The completion event object
 */
export const onNetworkEnd = (
    event: d3.Simulation<SimNode, undefined>,
): void => {
    dg.network.layers.link?.selectAll(".link").classed("noninteractive", false);
    dg.network.layers.node?.selectAll(".node").classed("noninteractive", false);
    dg.network.isRunningLayout = false;
    onTick(event);
};

interface RequestNetworkEventDetail {
    entityKey: string;
    pushHistory: boolean;
}

/**
 * Custom event for requesting a network layout
 * @extends CustomEvent
 */
export class RequestNetworkEvent extends CustomEvent<RequestNetworkEventDetail> {
    /**
     * Creates a new RequestNetworkEvent
     * @param {string} entityKey - The key of the entity to request
     * @param {boolean} pushHistory - Whether to push the network layout to the history stack
     */
    constructor(entityKey: string, pushHistory: boolean) {
        super("discograph:request-network", {
            bubbles: true,
            detail: {
                entityKey,
                pushHistory,
            },
        });
    }
}

interface SelectEntityEventDetail {
    entityKey: string;
    fixed: boolean;
}

/**
 * Custom event for selecting an entity
 * @extends CustomEvent
 */
export class SelectEntityEvent extends CustomEvent<SelectEntityEventDetail> {
    /**
     * Creates a new SelectEntityEvent
     * @param {string} entityKey - The key of the entity to select
     * @param {boolean} fixed - Whether to fix the entity position
     */
    constructor(entityKey: string, fixed: boolean) {
        super("discograph:select-entity", {
            bubbles: true,
            detail: {
                entityKey,
                fixed,
            },
        });
    }
}

/**
 * Custom event for resizing the network window
 * @extends CustomEvent
 */
export class ResizeEvent extends Event {
    /**
     * Creates a new ResizeEvent
     */
    constructor() {
        super("discograph:resize", {
            bubbles: true,
        });
    }
}

// Add type declarations for custom events
declare global {
    interface WindowEventMap {
        "discograph:request-network": RequestNetworkEvent;
        "discograph:select-entity": SelectEntityEvent;
        "discograph:resize": ResizeEvent;
    }
}
