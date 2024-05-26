// Reheat the simulation when drag starts, and fix the subject position.
function dg_network_dragstarted(event) {
    event.subject.fx = event.subject.x;
    event.subject.fy = event.subject.y;
    event.subject.dragx = event.subject.x;
    event.subject.dragy = event.subject.y;
    if (event.sourceEvent.type == 'mousedown') {
        dg_network_onNodeMouseDown(event.sourceEvent, event.subject);
    }
}

// Update the subject (dragged node) position during drag.
function dg_network_dragged(event) {
    event.subject.fx = event.x;
    event.subject.fy = event.y;
    if (event.subject.dragx != event.subject.x ||
        event.subject.dragy != event.subject.y) {
        event.subject.dragx = event.subject.x;
        event.subject.dragy = event.subject.y;
        if (!event.active) dg_network_forceLayout_restart();
    }
}

// Restore the target alpha so the simulation cools after dragging ends.
// Unfix the subject position now that it’s no longer being dragged.
function dg_network_dragended(event) {
    if (event.subject.dragx == event.subject.x &&
        event.subject.dragy == event.subject.y)
        return;
    if (!event.active) dg.network.forceLayout.alphaTarget(0);
    event.subject.fx = null;
    event.subject.fy = null;
    if (event.sourceEvent.type == 'mouseup') {
        dg_network_onNodeMouseDown(event.sourceEvent, event.subject);
    }
}

function dg_network_start() {
    dg.network.isRunningLayout = true;
    dg.network.tick = 0;
    $('#network-running')
                .addClass('glyphicon-animate glyphicon-refresh');
    dg.network.layers.link.selectAll('.link')
        .classed('noninteractive', false);
    dg.network.layers.node.selectAll('.node')
        .classed('noninteractive', false);
}

function dg_network_end(event) {
    $('#network-running')
                .removeClass('glyphicon-animate glyphicon-refresh');
    dg.network.layers.link.selectAll('.link')
        .classed('noninteractive', false);
    dg.network.layers.node.selectAll('.node')
        .classed('noninteractive', false);
    dg.network.isRunningLayout = false;
    dg_network_tick();
}

function dg_network_forceLayout_stop() {
    console.log("forceLayout_stop: ");
    dg.network.forceLayout.alpha(0);
}
