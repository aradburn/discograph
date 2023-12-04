function dg_svg_init() {
    d3.selection.prototype.moveToFront = function() {
        return this.each(function(){ this.parentNode.appendChild(this); });
    };
    var w = window,
        d = document,
        e = d.documentElement,
        g = d.getElementsByTagName('body')[0];
    dg.dimensions = [
        w.innerWidth || e.clientWidth || g.clientWidth,
        w.innerHeight|| e.clientHeight|| g.clientHeight,
    ];
    dg.zoomFactor = Math.min(dg.dimensions[0], dg.dimensions[1]) / 1024;
    dg.network.newNodeCoords = [
        dg.dimensions[0],
        dg.dimensions[1],
    ];
    d3.select("#svg")
        .attr("width", dg.dimensions[0] * 2)
        .attr("height", dg.dimensions[1] * 2);
    dg_svg_setupDefs();
    d3.select('#svg').call(tip);
}

function dg_svg_setupDefs() {
    var defs = d3.select("#svg").append("defs");
    // ARROWHEAD
    defs.append("marker")
        .attr("id", "arrowhead")
        .attr("viewBox", "-5 -5 10 10")
        .attr("refX", 4)
        .attr("refY", 0)
        .attr("markerWidth", 8)
        .attr("markerHeight", 8)
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
        .attr("markerWidth", 8)
        .attr("markerHeight", 8)
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
    var gradient = defs.append('radialGradient')
        .attr('id', 'radial-gradient');
    gradient.append('stop')
        .attr('offset', '0%')
        .attr('stop-color', '#333')
        .attr('stop-opacity', '1.0');
    gradient.append('stop')
        .attr('offset', '50%')
        .attr('stop-color', '#333')
        .attr('stop-opacity', '0.333');
    gradient.append('stop')
        .attr('offset', '75%')
        .attr('stop-color', '#333')
        .attr('stop-opacity', '0.111');
    gradient.append('stop')
        .attr('offset', '100%')
        .attr('stop-color', '#333')
        .attr('stop-opacity', '0.0');
}
