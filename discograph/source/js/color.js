function dg_color_class(d) {
    if (d.type == 'artist') {
        return dg_color_heatmap_class(d);
    } else {
        return dg_color_greyscale_class(d);
    }
}

function dg_color_greyscale_class(d) {
    return 'q' + d.distance + '-9';
//    var dist = d.distance == 0 ? 0 :
//               d.distance == 1 ? 1 : 2;
//    var radius = d.radius <= 8 ? 0 : d.radius <= 14 ? 1 : 2;
//    var value = dist * 3 + radius;
//    var color_class = 'q' + value + '-9';
//    return color_class;
}

//function dg_color_greyscale(d) {
//    var hue = 0;
//    var saturation = 0;
//    var lightness = (d.distance / (dg.network.data.maxDistance + 1));
//    return d3.hsl(hue, saturation, lightness).toString();
//}

function dg_color_heatmap_class(d) {
    return 'q' + ((d.distance * 2) + 1) + '-9';
//    var value = 0;
//    if (d.distance > 0) {
//        var dist = d.distance * 2;
//        var radius = d.radius <= 20 ? 1 : 0;
//        var value = dist + radius;
//    }
//    var color_class = 'q' + value + '-9';
//    return color_class;
}

//function dg_color_heatmap(d) {
////console.log("d: ", d);
//    var hue = d.distance == 0 ? 0 :
//              d.distance == 1 ? 22 :
//              d.distance == 2 ? 90 : 200;
//    var saturation = 0.2 + (d.links.length <= 2 ? 0.2 : d.links.length <= 7 ? 0.4 : 0.8);
//    var lightness = 0.4 + (d.radius <= 8 ? 0.4 : d.radius <= 14 ? 0.2 : 0.0);
////    console.log("hsl: ", hue, saturation, lightness);
//    return d3.hsl(hue, saturation, lightness).toString();
//}

//function dg_color_heatmap(d) {
//    var hue = ((d.distance / 12) * 360) % 360;
//    var variation_a = ((d.id % 5) - 2) / 20;
//    var variation_b = ((d.id % 9) - 4) / 80;
//    var saturation = 0.67 + variation_a;
//    var lightness = 0.5 + variation_b;
//    return d3.hsl(hue, saturation, lightness).toString();
//}

