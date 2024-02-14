function dg_color_class(d) {
    if (d.type == 'artist') {
        return dg_color_artist_class(d);
    } else {
        return dg_color_label_class(d);
    }
}

function dg_color_artist_class(d) {
    return 'q' + ((d.distance * 2) + 1) + '-9';
}

function dg_color_label_class(d) {
    return 'q' + ((d.distance * 2) + 2) + '-9';
}
