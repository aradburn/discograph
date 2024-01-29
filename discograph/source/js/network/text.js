LABEL_OFFSET_Y = 9

function dg_network_getNodeText(d) {
    var name = d.name;
    if (50 < name.length) {
        name = name.slice(0, 50) + "...";
    }
    if (dg.debug) {
        var pages = '[' + d.pages + ']';
        return pages + ' ' + name;
    }
    return name;
//    return name + dg_network_getNodeDebug(d);
}

function dg_network_getNodeDebug(d) {
    var links = d.links !== undefined ? d.links.length : 0;
    return " dist: " + d.distance +
           " rad: " + d.radius +
           " lnk: " + links +
           " miss: " + d.missing +
//           " clu: " + d.cluster +
           " col: " + dg_color_class(d);
}


function dg_network_onTextEnter(textEnter) {
    var textEnter = textEnter.append("g")
        .attr("id", function(d) {
            return d.key;
        })
        .attr("class", function(d) {
            var classes = [
                "node",
//                d.key,
                d.key.split('-')[0],
                ];
            return classes.join(" ");
        })
    textEnter.append("text")
        .attr("class", "outer")
//        .attr("dx", function(d) { return dg_network_getOuterRadius(d) + 3; })
//        .attr("dy", ".35em")
//        .attr("dy", "1.2em")
        .attr("dy", function(d) { return dg_network_getOuterRadius(d) + LABEL_OFFSET_Y; })
        .attr("width", function(d) { return dg_network_getOuterRadius(d) * 3; })
        .text(dg_network_getNodeText);
    textEnter.append("text")
        .attr("class", "inner")
//        .attr("dx", function(d) { return dg_network_getOuterRadius(d) + 3; })
//        .attr("dy", ".35em")
//        .attr("dy", "1.2em")
        .attr("dy", function(d) { return dg_network_getOuterRadius(d) + LABEL_OFFSET_Y; })
        .attr("width", function(d) { return dg_network_getOuterRadius(d) * 3; })
        .text(dg_network_getNodeText);
}

function dg_network_onTextExit(textExit) {
    textExit.remove();
}

function dg_network_onTextUpdate(textUpdate) {
    textUpdate.select('.outer').text(dg_network_getNodeText);
    textUpdate.select('.inner').text(dg_network_getNodeText);
}

