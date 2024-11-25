VIEWPORT_SIZE_MULTIPLIER = 3.0;
SVG_SCALING_MULTIPLIER = 0.8

$(document).ready(function() {
    dg_window_init();
    dg_svg_init();
    dg_network_init();
    dg_relations_init();
    dg_roles_init();
    dg_loading_init();
    dg_typeahead_init();

    $('#request-random').on("click touchstart", function(event) {
        event.preventDefault();
        $(this).tooltip('hide');
        $(this).trigger({
            type: 'discograph:request-random',
        });
    });
    $('#start-layout').on("click touchstart", function(event) {
        event.preventDefault();
        dg_network_forceLayout_restart();
    });
    $('#stop-layout').on("click touchstart", function(event) {
        event.preventDefault();
        dg_network_forceLayout_stop();
    });
    $('#print').on("click touchstart", function(event) {
        event.preventDefault();
        dg_svg_print(dg.svg_dimensions[0], dg.svg_dimensions[1]);
    });
//    $('#filter-roles').select2().on('select2:select', function(event) {
//        $(window).trigger({
//            type: 'discograph:request-network',
//            entityKey: dg.network.data.json.center.key,
//            pushHistory: true,
//        });
//    });
//    $('#filter-roles').select2().on('select2:unselect', function(event) {
//        $(window).trigger({
//            type: 'discograph:request-network',
//            entityKey: dg.network.data.json.center.key,
//            pushHistory: true,
//        });
//    });
//    $('#filter').fadeIn(3000);

    // Tooltip from Bootstrap
    $('[data-toggle="tooltip"]').tooltip();
    $(function () {
        $('[data-tooltip="tooltip"]').tooltip({
            trigger: 'hover'
        });
    });

    dg.fsm = new DiscographFsm();
    console.log('discograph initialized.');
});

function dg_window_init() {
    // Setup window dimensions
    var w = window,
        d = document,
        e = d.documentElement,
        g = d.getElementsByTagName('body')[0];
    dg.dpr = w.devicePixelRatio;
    console.log("window devicePixelRatio: ", dg.dpr);

    svgContainer = d.getElementById('svg-container-fluid');
    dg.dimensions = [
        svgContainer.clientWidth,
        svgContainer.clientHeight,
    ];
    console.log("svg panel dimensions: ", dg.dimensions);

    dg.svg_dimensions = [
        dg.dimensions[0] * VIEWPORT_SIZE_MULTIPLIER * dg.dpr,
        dg.dimensions[1] * VIEWPORT_SIZE_MULTIPLIER * dg.dpr,
    ];
    console.log("svg coord dimensions: ", dg.svg_dimensions);

    // All nodes start at center of the screen
    dg.network.newNodeCoords = [
        dg.svg_dimensions[0] / 2,
        dg.svg_dimensions[1] / 2,
    ];
    console.log("svg newNodeCoords: ", dg.network.newNodeCoords);
}

function dg_show_message(type, message) {
    var text = [
            '<div class="alert alert-' + type + ' alert-dismissible" role="alert">',
            '<button type="button" class="close" data-dismiss="alert" aria-label="Close">',
            '<span aria-hidden="true">&times;</span>',
            '</button>',
            message,
            '</div>'
            ].join('');
    $('#flash').append(text);
}

function dg_clear_messages(delay) {
    setTimeout(
        function() {
            $('#flash').empty();
        }, delay);
}
