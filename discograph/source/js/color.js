function dg_node_color_class(d) {
    if (d.type == 'artist') {
        return dg_node_color_artist_class(d);
    } else {
        return dg_node_color_label_class(d);
    }
}

function dg_node_color_artist_class(d) {
    var index = clamp(d.distance + 1, 0, 8);
    return 'color-' + index;
}

function dg_node_color_label_class(d) {
    var index = clamp(d.distance + 2, 0, 8);
    return 'color-' + index;
}

function dg_link_color_class(d) {
    var distance = Math.min(d.source.distance, d.target.distance);
    if (distance == 0) {
        distance = 2;
    } else {
        distance = 5;
    }
    var index = clamp(distance, 0, 8);
    return 'color-' + index;
}
