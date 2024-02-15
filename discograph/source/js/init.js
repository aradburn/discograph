VIEWPORT_SIZE_MULTIPLIER = 3;
SVG_SCALING_MULTIPLIER = 0.8

$(document).ready(function() {
    dg_window_init();
    dg_svg_init();
    dg_svg_container_setup();
    dg_network_init();
    dg_relations_init();
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
    $('#paging .next a').on("click", function(event) {
        $(this).trigger({
            type: 'discograph:select-next-page', 
        });
        $(this).tooltip('hide');
    });
    $('#paging .previous a').on("click", function(event) {
        $(this).trigger({
            type: 'discograph:select-previous-page',
        });
        $(this).tooltip('hide');
    });
    $('#filter-roles').select2().on('select2:select', function(event) {
        $(window).trigger({
            type: 'discograph:request-network',
            entityKey: dg.network.data.json.center.key,
            pushHistory: true,
        });
    });
    $('#filter-roles').select2().on('select2:unselect', function(event) {
        $(window).trigger({
            type: 'discograph:request-network',
            entityKey: dg.network.data.json.center.key,
            pushHistory: true,
        });
    });
    $('#filter').fadeIn(3000);

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

function dg_svg_container_setup() {
    var navTopHeight = $('#nav-top').height();
    var navBottomHeight = $('#nav-bottom').height();

    var containerWidth = Math.floor(dg.dimensions[0] / (VIEWPORT_SIZE_MULTIPLIER * dg.dpr));
    var containerHeight = Math.floor(dg.dimensions[1] / (VIEWPORT_SIZE_MULTIPLIER * dg.dpr));
    console.log("svg_container dimensions: ", containerWidth, containerHeight);

    $('#svg-container').width(containerWidth);
    $('#svg-container').height(containerHeight - navBottomHeight);

    // Set the initial scroll bars so that the viewport is in the centre of the larger svg canvas
    var containerOffsetX = (VIEWPORT_SIZE_MULTIPLIER * dg.dpr - 1) * containerWidth / 2;
    var containerOffsetY = (VIEWPORT_SIZE_MULTIPLIER * dg.dpr - 1) * containerHeight / 2;
    $('#svg-container').scrollLeft(containerOffsetX);
    $('#svg-container').scrollTop(containerOffsetY);
    console.log("svg_container scroll offsets: ", containerOffsetX, containerOffsetY);
}

function dg_window_init() {
    // Setup window dimensions
    var w = window,
        d = document,
        e = d.documentElement,
        g = d.getElementsByTagName('body')[0];
    dg.dpr = w.devicePixelRatio;
    console.log("window devicePixelRatio: ", dg.dpr);

    dg.dimensions = [
        Math.floor((w.innerWidth || e.clientWidth || g.clientWidth) * VIEWPORT_SIZE_MULTIPLIER * dg.dpr),
        Math.floor((w.innerHeight|| e.clientHeight|| g.clientHeight) * VIEWPORT_SIZE_MULTIPLIER * dg.dpr),
    ];
    console.log("window dimensions: ", dg.dimensions);

    dg.svg_dimensions = [
        dg.dimensions[0] * SVG_SCALING_MULTIPLIER,
        dg.dimensions[1] * SVG_SCALING_MULTIPLIER,
    ];
    console.log("svg dimensions: ", dg.svg_dimensions);

    // All nodes start at center of the screen
    dg.network.newNodeCoords = [
        dg.svg_dimensions[0] / 2,
        dg.svg_dimensions[1] / 2,
    ];
    console.log("newNodeCoords: ", dg.network.newNodeCoords);
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
