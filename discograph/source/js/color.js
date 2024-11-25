function dg_color_class(d) {
    if (d.type == 'artist') {
        return dg_color_artist_class(d);
    } else {
        return dg_color_label_class(d);
    }
}

function clamp(num, lower, upper) {
    return Math.min(Math.max(num, lower), upper);
}

function dg_color_artist_class(d) {
    var index = clamp(d.distance + 1, 0, 8);
//    var index = clamp((d.distance * 2) + 1, 0, 8);
    return 'q' + index + '-9';
}

function dg_color_label_class(d) {
    var index = clamp(d.distance + 2, 0, 8);
//    var index = clamp((d.distance * 2) + 2, 0, 8);
    return 'q' + index + '-9';
}
