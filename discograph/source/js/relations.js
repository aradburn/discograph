dg.relations = {
    layers: {
        root: null,
        },
    };

function dg_relations_init() {
    var svgElement = d3.select("#svg");
    var root = svgElement.append("g").attr("id", "relationsLayer");
    dg.relations.layers.root = root;

//    dg.relations.zoom = d3.zoom()
//        .extent([[0, 0], [dg.svg_dimensions[0], dg.svg_dimensions[1]]])
//        .scaleExtent([1, 8])
//        .on("zoom", dg_relations_zoomed);
//    svgElement.call(dg.relations.zoom)
}

function dg_relations_chartRadial() {
    console.log("dg_relations_chartRadial()");
    var textAnchor = function(d, i) {
        var angle = (i + 0.5) / numBars;
        if (angle < 0.5) {
            return 'start';
        } else {
            return 'end';
        }
    };
    console.log("dg_relations_chartRadial() a");

    var barHeight = d3.min(dg.dimensions) / 3;
    console.log("dg_relations_chartRadial() barHeight:", barHeight);
    var data = dg.relations.byRole;
    console.log("dg_relations_chartRadial() data: ", data);

    var extent = d3.extent(data, function(d) { return d[1]; });
    console.log("dg_relations_chartRadial() extent: ", extent);

//    var barScale = d3.scaleSqrt()
//        .exponent(0.25)
//        .domain(extent)
//        .range([barHeight / 4, barHeight]);
    var barScale = d3.scaleSqrt(extent, [barHeight / 4, barHeight])
            .exponent(0.25);
    var numBars = data.size;

    var transform = function(d, i) {
        console.log("d: ", d);
        console.log("i: ", i);
        var hypotenuse = barScale(d[1]) + 5;
//        console.log("hypotenuse: ", hypotenuse);
        var angle = (i + 0.5) / numBars;
//        console.log("angle: ", angle);
        var degrees = (angle * 360);
//        console.log("degrees: ", degrees);
        if (180 <= degrees) {
            degrees -= 180;
        }
        degrees -= 90;
//        console.log("degrees: ", degrees);
        var radians = angle * 2 * Math.PI;
//        console.log("radians: ", radians);
        var x = Math.sin(radians) * hypotenuse;
        var y = - Math.cos(radians) * hypotenuse;
        return [
            'rotate(' + degrees + ',' + x + ',' + y + ')',
            'translate(' + x +',' + y + ')'
            ].join(' ');
    }

    dg_relations_init();

    var arc = d3.arc()
        .startAngle(function(d,i) { return (i * 2 * Math.PI) / numBars; })
        .endAngle(function(d,i) { return ((i + 1) * 2 * Math.PI) / numBars; })
        .innerRadius(0);
    console.log("dg_relations_chartRadial() arc: ", arc);

    dg.arc = arc;
    var radialGroup = dg.relations.layers.root.append('g')
        .attr('class', 'radial centered')
        .attr('transform', 'translate(' +
            (dg.dimensions[0] / 2) +
            ',' +
            (dg.dimensions[1] / 2) +
            ')'
            );
    // TODO var selectedRoles = $('#filter select').val();
    var segments = radialGroup.selectAll('g')
        .data(data)
        .enter()
        .append('g')
        .attr('class', 'segment')
//  TODO      .classed('selected', function(d) {
//            return selectedRoles.indexOf(d.key) != -1;
//        })
        .on('mouseover', function(event) { d3.select(this).raise(); });
    console.log("dg_relations_chartRadial() segments: ", segments)
    var arcs = segments.append('path')
        .attr('class', 'arc')
        .attr('d', arc)
        .each(function(d, i) { d.outerRadius = 0; })
//        .on('mousedown', function(event, d) {
//            var values = $('#filter-roles').val();
//            values.push(d.key);
//            $('#filter-roles').val(values).trigger('change');
//            dg.fsm.requestNetwork(dg.network.data.json.center.key, true);
//            event.stopPropagation();
//        });
        .transition()
        .ease(d3.easeElastic)
        .duration(500)
        .delay(function(d, i) { return (numBars - i) * 25; })
        .attrTween('d', function(d, i) {
            console.log("attrTween d: ", d);
            var outer = d3.interpolate(d.outerRadius, barScale(d[1]));
            return function(t) {
                console.log("attrTween t: ", t);
                d.outerRadius = outer(t);
                var result = arc(d, i);
                console.log("attrTween result: ", result);
                return result;
            };
        });
    console.log("dg_relations_chartRadial() arcs: ", arcs)
    var outerLabels = segments.append('text')
        .attr('class', 'outer')
        .attr('text-anchor', textAnchor)
        .attr('transform', transform)
        .text(function(d) { return d[0]; });
    var innerLabels = segments.append('text')
        .attr('class', 'inner')
        .attr('text-anchor', textAnchor)
        .attr('transform', transform)
        .text(function(d) { return d[0]; });
    console.log("dg_relations_chartRadial() end")
}

function dg_relations_zoomed({transform}) {
    dg.relations.layers.root.attr("transform", transform);
}