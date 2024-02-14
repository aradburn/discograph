dg.loading = {};

function dg_loading_init() {
    var layer = d3.select('#svg')
        .append('g')
        .attr('id', 'loadingLayer')
        .attr('class', 'centered')
        .attr('transform', 'translate(' +
            dg.dimensions[0] / 2 +
            ',' +
            dg.dimensions[1] / 2 +
            ')'
            );
    dg.loading.arc = d3.arc()
        .startAngle(function(d) { return d.startAngle; })
        .endAngle(function(d) { return d.endAngle; })
        .innerRadius(function(d) { return d.innerRadius; })
        .outerRadius(function(d) { return d.outerRadius; });
    dg.loading.barHeight = 200;
    dg.loading.layer = layer; 
    dg.loading.selection = layer.selectAll('path');
}

function dg_loading_toggle(status) {
    if (status) {
        var input = dg_loading_makeArray();
        var data = input[0], extent = input[1];
        $("#page-loading")
            .addClass("glyphicon-animate glyphicon-refresh");
    } else {
        var data = [], extent = [0, 0];
        $("#page-loading")
            .removeClass("glyphicon-animate glyphicon-refresh")
    }
    dg_loading_update(data, extent);
}

function dg_loading_makeArray() {
    var count = 10;
    var values = [];
    var data = [];
    for (var i = 0; i < count; i++) {
        var pair = [Math.random(), Math.random()];
        pair.sort();
        values.push(pair[0]);
        values.push(pair[1]);
        data.push({
            active: true,
            startAngle: 2 * Math.PI * Math.random(),
            endAngle: 2 * Math.PI * Math.random(),
            rotationRate: Math.random() * 10,
            targetInnerRadius: pair[0],
            targetOuterRadius: pair[1],
        });
    }
    return [data, d3.extent(values)];
}

function dg_loading_update(data, extent) {
    var barScale = d3.scaleLinear()
        .domain(extent)
        .range([
            dg.loading.barHeight / 4, 
            dg.loading.barHeight
        ]);

    var dataSelection = dg.loading.layer.selectAll('path')
        .data(data);

    var selectionEnter = dataSelection.enter()
        .call(dg_loading_transition_enter);
    dataSelection.exit()
        .call(dg_loading_transition_exit);

    dataSelection = dg.loading.layer.selectAll('path')
        .data(data);
    dg_loading_transition_update(dataSelection, barScale);
    if (selectionEnter.size() > 0) {
        dg_loading_rotate(dataSelection);
    }
}

//function dg_loading_update(data, extent) {
//    var barScale = d3.scaleLinear()
//        .domain(extent)
//        .range([
//            dg.loading.barHeight / 4,
//            dg.loading.barHeight
//        ]);
//    dg.loading.selection = dg.loading.layer.selectAll('path');
//    dg.loading.selection = dg.loading.selection.data(data);
////        data,
////        function(d) { return d; });
//    var scale = d3.scaleOrdinal(d3.schemeCategory10);
//    var selectionEnter = dg.loading.selection
//        .enter()
//        .append('path')
//        .attr('class', 'arc')
//        .attr('d', dg.loading.arc)
//        .attr('fill', function(d, i) {
//            return scale(i);
//        })
//        .each(function(d, i) {
//            d.innerRadius = 0;
//            d.outerRadius = 0;
//            d.hasTimer = false;
//        })
//        .merge(dg.loading.selection);
//    var selectionExit = dg.loading.selection
//        .exit();
//    dg_loading_transition_update(selectionEnter, barScale);
//    dg_loading_transition_exit(selectionExit);
//    if (selectionEnter.size() > 0) {
//        dg_loading_rotate(selectionEnter);
//    }
//}

function dg_loading_transition_enter(selection) {
    var scale = d3.scaleOrdinal(d3.schemeCategory10);
    selection
        .append('path')
        .attr('class', 'arc')
        .attr('d', dg.loading.arc)
        .attr('fill', function(d, i) {
            return scale(i);
        })
        .each(function(d, i) {
            d.innerRadius = 0;
            d.outerRadius = 0;
            d.hasTimer = false;
        });
}

function dg_loading_transition_update(selection, barScale) {
    selection
        .transition()
        .duration(1000)
        .delay(function(d, i) { return (selection.size() - i) * 100; })
        .attrTween('d', function(d, i) {
            var inner = d3.interpolate(d.innerRadius, barScale(d.targetInnerRadius));
            var outer = d3.interpolate(d.outerRadius, barScale(d.targetOuterRadius));
            return function(t) {
                d.innerRadius = inner(t);
                d.outerRadius = outer(t);
                return dg.loading.arc(d, i);
            };
        });
}

function dg_loading_transition_exit(selection) {
    return selection
        .transition()
        .duration(1000)
        .delay(function(d, i) { return (selection.size() - i) * 100; })
        .attrTween('d', function(d, i) {
            var inner = d3.interpolate(d.innerRadius, 0);
            var outer = d3.interpolate(d.outerRadius, 0);
            return function(t) {
                d.innerRadius = inner(t);
                d.outerRadius = outer(t);
                return dg.loading.arc(d, i);
            };
        })
        .on('end', function(d) {
            d.active = false;
            this.remove();
        });
}

function dg_loading_rotate(selection) {
    selection
        .each(function(d) {
            if (d.hasTimer) {
                return;
            }
            d.hasTimer = true;
            d.timer = d3.interval(function(elapsed) {
                if (!d.active) {
                    console.log("stop timer");
                    d.timer.stop();
                    d.hasTimer = false;
                    d.timer = null;
                }
//                console.log("element: ", element);
                selection.attr('transform', function() {
                    var angle = elapsed * d.rotationRate;
                    if (0 < d.outerRadius) {
                        angle = angle / d.outerRadius;
                    }
                    return 'rotate(' + angle + ')';
                });
            }, 20);
        });
}