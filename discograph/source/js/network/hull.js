function dg_network_onHullEnter(hullEnter) {
console.log("hullEnter", hullEnter);
    var hullGroup = hullEnter
        .append("g")
        .attr("class", function(d) { return "hull hull-" + d.key });
    hullGroup.append("path");
}

function dg_network_onHullExit(hullExit) {
    hullExit.remove();
}

