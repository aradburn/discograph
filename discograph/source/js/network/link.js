LINK_DEBOUNCE_TIME = 250
LINK_OUT_TRANSITION_TIME = 500


/* Initialize link tooltip */
var linkToolTip = d3.tip()
    .attr('class', 'd3-link-tooltip')
    .direction('n')
//    .rootElement(e => )
    .offset([20, 0])
    .html(dg_network_link_tooltip);

function dg_network_onLinkEnter(linkEnter) {
    var linkEnter = linkEnter.append("g")
        .attr("id", function(d) {
            return "link-" + d.key;
        })
        .attr("class", function(d) {
            var parts = d.key.split('-');
            var role = parts.slice(2, 2 + parts.length - 4).join('-')
            var classes = [
                "link",
//                "link-" + d.key,
                role,
                ];
            return classes.join(" ");
        });
    dg_network_onLinkEnterElementConstruction(linkEnter);
    dg_network_onLinkEnterEventBindings(linkEnter);
}

function dg_network_onLinkEnterElementConstruction(linkEnter) {
    linkEnter
        .append("path")
        .attr("class", "inner");
    linkEnter
        .append("text")
        .attr('class', 'outer')
        .text(dg_network_linkAnnotation);
    linkEnter
        .append("text")
        .attr('class', 'inner')
        .text(dg_network_linkAnnotation);
}

function dg_network_onLinkEnterEventBindings(linkEnter) {
    var debounceToolTip = $.debounce(LINK_DEBOUNCE_TIME, function(self, d, status) {
        if (status) {
        //console.log("link: ", self, d);
            linkToolTip.show(d, d3.select(self).select('text').node());
        } else {
            linkToolTip.hide(d);
        }
    });
    linkEnter.on("mouseover", function(event, d) {
        d3.select(this)
            .classed("selected", true);
        debounceToolTip(this, d, true);
    });
    linkEnter.on("mouseout", function(event, d) {
        d3.select(this)
            .classed("selected", false)
            .transition()
            .duration(LINK_OUT_TRANSITION_TIME);
        debounceToolTip(this, d, false);
    });
}

function dg_network_link_tooltip(d) {
    var parts = [
        '<div>' + d.source.name + '</div>',
        '<div>' + d.role + '</div>',
        '<div>' + d.target.name + '</div>',
        ];
//    var parts = [
//        '<span class="link-label-top">' + d.source.name + '</span><br/>',
//        '<span class="link-label-middle">' + d.role + '</span><br/>',
//        '<span class="link-label-bottom">' + d.target.name + '</span>',
//        ];
    return parts.join('');
}

//function dg_network_tooltip(d) {
//    var topBackgroundColor = (d.source.type == 'artist') ? dg_color_heatmap(d.source) : dg_color_greyscale(d.source);
//    var bottomBackgroundColor = (d.target.type == 'artist') ? dg_color_heatmap(d.target) : dg_color_greyscale(d.target);
//
//    var parts = [
//        '<span class="link-label-top" style="border-color:' + topBackgroundColor + '">' + d.source.name + '</span><br/>',
//        '<span class="link-label-middle">' + d.role + '</span><br/>',
//        '<span class="link-label-bottom" style="border-color:' + bottomBackgroundColor + '">' + d.target.name + '</span>',
//        ];
//    return parts.join('');
//}

function dg_network_linkAnnotation(d) {
    return d.role.split(' ').map(function(x) { return x[0]; }).join('');
}

function dg_network_onLinkExit(linkExit) {
    linkExit.remove();
}

function dg_network_onLinkUpdate(linkSelection) {
}