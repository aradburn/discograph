/**
 * Network Graph Event Handlers
 * This file contains event handlers and control functions for a force-directed network graph,
 * implemented using D3.js. It manages node dragging behavior and layout controls.
 */

// Import dependencies
import { dg } from '../dg';
import { onNodeMouseDown } from './node.js';
import { onTick } from './tick.js';
import { restartForceLayout, stopForceLayout } from './forceLayout.js';

/**
 * Reheat the simulation when drag starts, and fix the subject position.
 * @param {d3.D3DragEvent} event - The drag event object
 */
export const onDragStart = (event) => {
    event.subject.fx = event.subject.x;
    event.subject.fy = event.subject.y;
    event.subject.dragx = event.subject.x;
    event.subject.dragy = event.subject.y;
    if (event.sourceEvent.type === 'mousedown') {
        onNodeMouseDown(event.sourceEvent, event.subject);
    }
}

/**
 * Updates node position during drag operation
 * @param {d3.D3DragEvent} event - The drag event object
 */
export const onDrag = (event) => {
    event.subject.fx = event.x;
    event.subject.fy = event.y;
    if (event.subject.dragx !== event.subject.x ||
        event.subject.dragy !== event.subject.y) {
        event.subject.dragx = event.subject.x;
        event.subject.dragy = event.subject.y;
        if (!event.active) restartForceLayout();
    }
}

/**
 * Handles the end of a node drag operation
 * Releases the fixed position of the node and allows the simulation to continue
 * @param {d3.D3DragEvent} event - The drag event object
 */
export const onDragEnd = (event) => {
    if (event.subject.dragx === event.subject.x &&
        event.subject.dragy === event.subject.y) {
        return;
    }
    if (!event.active) stopForceLayout();
    event.subject.fx = null;
    event.subject.fy = null;
    if (event.sourceEvent.type === 'mouseup') {
        onNodeMouseDown(event.sourceEvent, event.subject);
    }
}

/**
 * Initiates the network layout simulation
 * Shows the running indicator and enables interaction with nodes and links
 */
export const onNetworkStart = () => {
    dg.network.isRunningLayout = true;
    dg.network.tick = 0;
    // const runningIndicator = document.getElementById('network-running');
    // runningIndicator.style.display = 'block';
    
    dg.network.layers.link?.selectAll('.link')
        .classed('noninteractive', false);
    dg.network.layers.node?.selectAll('.node')
        .classed('noninteractive', false);
}

/**
 * Handles the completion of network layout simulation
 * Hides the running indicator and ensures nodes and links remain interactive
 * @param {Object} event - The completion event object
 */
export const onNetworkEnd = (event) => {
    // const runningIndicator = document.getElementById('network-running');
    // runningIndicator.style.display = 'none';
    
    dg.network.layers.link?.selectAll('.link')
        .classed('noninteractive', false);
    dg.network.layers.node?.selectAll('.node')
        .classed('noninteractive', false);
    dg.network.isRunningLayout = false;
    onTick(event);
}

/**
 * Custom event for requesting a network layout
 * @extends CustomEvent
 */
export class RequestNetworkEvent extends CustomEvent {
    /**
     * Creates a new RequestNetworkEvent
     * @param {string} entityKey - The key of the entity to request
     * @param {boolean} pushHistory - Whether to push the network layout to the history stack
     */
    constructor(entityKey, pushHistory) {
        super("discograph:request-network", {
            bubbles: true,
            detail: {
                entityKey: entityKey,
                pushHistory: pushHistory
            }
        });
    }
}

/**
 * Custom event for selecting an entity
 * @extends CustomEvent
 */
export class SelectEntityEvent extends CustomEvent {
    /**
     * Creates a new SelectEntityEvent
     * @param {string} entityKey - The key of the entity to select
     * @param {boolean} fixed - Whether to fix the entity position
     */
    constructor(entityKey, fixed) {
        super("discograph:select-entity", {
            bubbles: true,
            detail: {
                entityKey: entityKey,
                fixed: fixed
            }
        });
    }
}