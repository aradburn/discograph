NODE_INNER_RADIUS = 8
NODE_OUTER_RADIUS = 11

function dg_network_getRadius(d) {
    var boost1 = d.distance === 0 ? 10 : d.distance === 1 ? 5 : 0;
    var boost2 = d.links && d.links.length >= 20 ? 10 : d.links && d.links.length >= 10 ? 5 : 0;
    var alias = (d.cluster !== undefined) ? 2 : 1;
    return Math.round(((Math.sqrt(d.size) * 2) + boost1 + boost2) / alias);
}

function dg_network_getOuterRadius(d) {
    return NODE_OUTER_RADIUS + dg_network_getRadius(d);
}

function dg_network_getInnerRadius(d) {
    return NODE_INNER_RADIUS + dg_network_getRadius(d);
}

function dg_network_splineInner(sX, sY, sR, cX, cY) {
    var dX = (sX - cX),
        dY = (sY - cY);
    var angle = Math.atan(dY / dX);
    dX = Math.abs(Math.cos(angle) * sR);
    dY = Math.abs(Math.sin(angle) * sR);
    sX = (sX < cX) ? sX + dX : sX - dX;
    sY = (sY < cY) ? sY + dY : sY - dY;
    return [sX, sY];
}

function dg_network_spline(d) {
    var sX = d.source.x;
    var sY = d.source.y;
    var tX = d.target.x;
    var tY = d.target.y;
    var tR = d.target.radius;
    var sR = d.source.radius;
    if (d.intermediate) {
        var cX = d.intermediate.x;
        var cY = d.intermediate.y;
        sXY = dg_network_splineInner(sX, sY, sR, cX, cY);
        tXY = dg_network_splineInner(tX, tY, tR, cX, cY);
        return (
            'M ' + sXY[0] + ',' + sXY[1] + ' ' +
            'S ' + cX + ',' + cY + ' ' +
            ' ' + tXY[0] + ',' + tXY[1] + ' '
            );
    } else {
        return 'M ' + [sX, sY] + ' L ' + [tX, tY];
    }
}

function dg_network_getHullVertices(nodes) {
    var vertices = [];
    nodes.forEach(function(d) {
        var radius = d.radius / 3;
        vertices.push([d.x + radius, d.y + radius]);
        vertices.push([d.x + radius, d.y - radius]);
        vertices.push([d.x - radius, d.y + radius]);
        vertices.push([d.x - radius, d.y - radius]);
    });
    return vertices;
}

var unlabeled_roles = [
    'Alias',
    'Member Of',
    'Sublabel Of',
    ];

function dg_network_tick_link(d, i) {
    var group = d3.select(this);
    var path = group.select('path');
    path.attr('d', dg_network_spline(d));
    path.classed('distance-0', d.source.distance == 0);
    path.classed('distance-1', d.source.distance == 1);
    path.classed('distance-2', d.source.distance == 2);
    var x1 = d.source.x,
        y1 = d.source.y,
        x2 = d.target.x,
        y2 = d.target.y;
    var node = path.node();
    if (node && node.getTotalLength() > 0) {
        var point = node.getPointAtLength(node.getTotalLength() / 2);
        var angle = Math.atan2((y2 - y1), (x2 - x1)) * (180 / Math.PI);
        var text = group.selectAll('text')
            .attr('transform', [
                'rotate(' + angle + ' ' + point.x + ' ' + point.y + ')',
                'translate(' + point.x + ',' + point.y + ')',
                ].join(' ')
                );
    }
}

function dg_network_translate(d) {
    return 'translate(' + d.x + ',' + d.y + ')';
}

function dg_network_tick(e) {
    dg.network.tick += 1;
    console.log("tick: ", dg.network.tick);
    var k = 1.0; //e.alpha * 5;
    if (dg.network.data.json) {
        var centerNode = dg.network.data.nodeMap.get(dg.network.data.json.center.key);
        if (!centerNode.fixed) {
            var dx = ((dg.dimensions[0] / 2) - centerNode.x) * k;
            var dy = ((dg.dimensions[1] / 2) - centerNode.y) * k;
            centerNode.x += dx;
            centerNode.y += dy;
        }
    }
    dg.network.layers.link
        .selectAll(".link")
        .each(dg_network_tick_link);
    dg.network.layers.halo
        .selectAll(".node")
        .attr('transform', dg_network_translate);
    dg.network.layers.node
        .selectAll(".node")
        .attr('transform', dg_network_translate);
    dg.network.layers.text
        .selectAll(".node")
        .attr('transform', dg_network_translate);
    dg.network.layers.halo
        .selectAll(".hull")
        .select('path')
        .attr('d', function(d) {
            var vertices = d3.polygonHull(dg_network_getHullVertices(d.flat()));
            return 'M' + vertices.join('L') + 'Z';
        });

}