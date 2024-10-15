function dg_network_init() {
    var svgElement = d3.select("#svg");
    var root = svgElement.append("g").attr("id", "networkLayer");
    dg.network.layers.root = root;
    dg.network.layers.halo = root.append("g").attr("id", "haloLayer");
    dg.network.layers.link = root.append("g").attr("id", "linkLayer");
    dg.network.layers.node = root.append("g").attr("id", "nodeLayer");
    dg.network.layers.text = root.append("g").attr("id", "textLayer");
    dg.network.selections.halo = dg.network.layers.halo.selectAll(".node");
    dg.network.selections.hull = dg.network.layers.halo.selectAll(".hull");
    dg.network.selections.link = dg.network.layers.link.selectAll(".link");
    dg.network.selections.node = dg.network.layers.node.selectAll(".node");
    dg.network.selections.text = dg.network.layers.text.selectAll(".node");

    dg.network.zoom = d3.zoom()
        .extent([[0, 0], [dg.svg_dimensions[0], dg.svg_dimensions[1]]])
        .scaleExtent([1, 8])
        .on("zoom", dg_network_zoomed);
    svgElement.call(dg.network.zoom)

    var t = d3.zoomIdentity.scale(VIEWPORT_SIZE_MULTIPLIER).translate(-dg.svg_dimensions[0] / VIEWPORT_SIZE_MULTIPLIER, -dg.svg_dimensions[1] / VIEWPORT_SIZE_MULTIPLIER);
    svgElement.transition().duration(0).call(dg.network.zoom.transform, t);
    dg.network.forceLayout = dg_network_setupForceLayout();
}

function dg_network_reset_transform() {
    var svgElement = d3.select("#svg");
    var t = d3.zoomIdentity.scale(VIEWPORT_SIZE_MULTIPLIER).translate(-dg.svg_dimensions[0] / VIEWPORT_SIZE_MULTIPLIER, -dg.svg_dimensions[1] / VIEWPORT_SIZE_MULTIPLIER);
    svgElement.transition().duration(750).call(
          dg.network.zoom.transform,
          t,
          d3.zoomTransform(svgElement.node()).invert([dg.svg_dimensions[0] / VIEWPORT_SIZE_MULTIPLIER, dg.svg_dimensions[1] / VIEWPORT_SIZE_MULTIPLIER])
        );
}

function dg_network_zoomed({transform}) {
    dg.network.layers.root.attr("transform", transform);
    dg_network_node_check_tooltip()
}