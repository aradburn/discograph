var DiscographFsm = machina.Fsm.extend({
    initialize: function(options) {
        var self = this;
        $(window).on('discograph:request-network', function(event) {
            self.requestNetwork(event.entityKey, event.pushHistory);
        });
        $(window).on('discograph:request-random', function(event) {
            self.requestRandom();
        });
        $(window).on('discograph:select-entity', function(event) {
            self.selectEntity(event.entityKey, event.fixed);
        });
        $(window).on('discograph:select-next-page', function(event) {
            self.selectNextPage();
        });
        $(window).on('discograph:select-previous-page', function(event) {
            self.selectPreviousPage();
        });
        $(window).on('discograph:show-network', function(event) {
            self.showNetwork();
        });
        $(window).on('discograph:show-radial', function(event) {
            self.showRadial();
        });
        $(window).on('select2:selecting', function(event) {
            self.rolesBackup = $('#filter select').val();
        });
        $(window).on('select2:unselecting', function(event) {
            self.rolesBackup = $('#filter select').val();
        });
        window.onpopstate = function(event) {
            if (!event || !event.state || !event.state.key) {
                return;
            }
            var entityKey = event.state.key;
            var entityType = entityKey.split("-")[0];
            var entityId = entityKey.split("-")[1];
            var url = "/" + entityType + "/" + entityId;
            // ### TODO setup analytics ga('send', 'pageview', url);
            // ### TODO setup analytics ga('set', 'page', url);
            $(window).trigger({
                type: 'discograph:request-network',
                entityKey: event.state.key,
                pushHistory: false,
            });
        };
        $(window).on('resize', $.debounce(100, function(event) {
            dg_window_init();
            dg_svg_container_setup();
            dg_svg_set_size();

            var transform = [
                'translate(',
                (dg.dimensions[0] / 2),
                ',',
                (dg.dimensions[1] / 2),
                ')'
                ].join('');
            d3.selectAll('.centered')
                .transition()
                .duration(250)
                .attr('transform', transform);

            if (self.state == 'viewing-network') {
                console.log("start d3 layout");
//                dg_network_processJson(dg.network.data.json);
//                dg_network_selectPage(1);
//                dg_network_startForceLayout();
                dg_network_forceLayout_restart(ALPHA / 10.0);
            }
        }));
        $('#svg').on('mousedown', function(event) {
            if (self.state == 'viewing-network') {
                self.selectEntity(null);
            } else if (self.state == 'viewing-radial') {
                self.showNetwork();
            }
        });
        self.on("*", function (event, data){
           // console.log("FSM: ", event, data);
        });
        this.loadInlineData();
        this.toggleRadial(false);
        self.rolesBackup = $('#filter select').val();
    },
    namespace: 'discograph',
    initialState: 'uninitialized',
    states: {
        'uninitialized': {
            'request-network': function(entityKey) {
                console.log("UNINITIALIZED request-network");
                this.requestNetwork(entityKey);
            },
            'request-random': function() {
                this.requestRandom();
            },
            'load-inline-data': function() {
                console.log("UNINITIALIZED load-inline-data");
                var params = {'roles': $('#filter select').val()};
//                this.handle("received-network", dgData, false, params);
                this.deferAndTransition("requesting");
                this.handle("received-network", dgData, false, params);
                console.log("load-inline-data end");
            },
        },
        'viewing-network': {
            '_onEnter': function() {
                console.log("VIEWING-NETWORK _onEnter");
                this.toggleNetwork(true);
            },
            '_onExit': function() {
                console.log("VIEWING-NETWORK _onExit");
                this.toggleNetwork(false);
            },
            'request-network': function(entityKey) {
                console.log("VIEWING-NETWORK request-network");
                this.requestNetwork(entityKey);
            },
            'request-random': function() {
                this.requestRandom();
            },
            'show-radial': function() {
                if (dg.network.pageData.selectedNodeKey) {
                    this.requestRadial(dg.network.pageData.selectedNodeKey);
                }
            },
            'select-entity': function(entityKey, fixed) {
                console.log("VIEWING-NETWORK select-entity", entityKey, fixed);
                dg.network.pageData.selectedNodeKey = entityKey;
                if (entityKey !== null) {
                    var selectedNode = dg.network.data.nodeMap.get(entityKey);
                    var currentPage = dg.network.pageData.currentPage;
                    if (selectedNode.pages.indexOf(currentPage) == -1) {
                        dg.network.pageData.selectedNodeKey = null;
                    }
                }
                entityKey = dg.network.pageData.selectedNodeKey;
                if (entityKey !== null) {
                    var nodeOn = dg.network.layers.root.selectAll('#' + entityKey);
                    var nodeOff = dg.network.layers.root.selectAll('.node:not(#' + entityKey + ')');
//                    var nodeOn = dg.network.layers.root.selectAll('.' + entityKey);
//                    var nodeOff = dg.network.layers.root.selectAll('.node:not(.' + entityKey + ')');
                    var linkKeys = nodeOn.datum().links;
                    var linkOn = dg.network.selections.link.filter(function(d) {
                        return linkKeys.indexOf(d.key) >= 0;
                    });
                    var linkOff = dg.network.selections.link.filter(function(d) {
                        return linkKeys.indexOf(d.key) == -1;
                    });
                    var node = dg.network.data.nodeMap.get(entityKey);
                    var url = 'http://discogs.com/' + node.type + '/' + node.id;
                    $('#entity-name').text(node.name);
                    $('#entity-link').attr('href', url);
                    $('#entity-details').removeClass('hidden').show(0);
                    nodeOn.raise();
                    nodeOn.classed('selected', true);
                    if (fixed) {
                        //nodeOn.each(function(d) { d.fixed = true; });
                        node.fixed = true;
                    }
                    linkOn.classed('selected', true);
                } else {
                    var nodeOff = dg.network.layers.root.selectAll('.node');
                    var linkOff = dg.network.selections.link;
                    $('#entity-details').hide();
                }
                if (nodeOff) {
                    nodeOff.classed('selected', false);
                    nodeOff.each(function(d) { d.fixed = false; });
                }
                if (linkOff) {
                    linkOff.classed('selected', false);
                }
            },
        },
        'viewing-radial': {
            '_onEnter': function() {
                this.toggleRadial(true);
                d3.select('#relationsLayer').remove();
                dg_relations_chartRadial();
            },
            '_onExit': function() {
                d3.select('#relationsLayer').remove();
                this.toggleRadial(false);
            },
            'request-network': function(entityKey) {
                this.requestNetwork(entityKey);
            },
            'request-random': function() {
                this.requestRandom();
            },
            'show-network': function() {
                this.transition('viewing-network');
            },
        },
        'requesting': {
            '_onEnter': function(fsm, entityKey, pushHistory) {
                console.log("REQUESTING _onEnter");
                this.toggleLoading(true);
                this.toggleFilter(false);
            },
            '_onExit': function() {
                console.log("REQUESTING _onExit");
                this.toggleLoading(false);
                this.toggleFilter(true);
            },
            'errored': function(error) {
                this.handleError(error);
            },
            'received-network': function(data, pushHistory, params) {
                console.log("REQUESTING received network", data);
                var params = {'roles': $('#filter select').val()};
                var entityKey = data.center.key;
                dg.network.data.json = JSON.parse(JSON.stringify(data));
                document.title = 'Discograph2: ' + data.center.name;
                $(document).attr('body').id = entityKey;
                if (pushHistory === true) {
                    this.pushState(entityKey, params);
                }
                dg.network.data.pageCount = data.pages;
                dg.network.pageData.currentPage = 1;
                if (data.pages > 1) {
                    $('#paging').fadeIn();
                } else {
                    $('#paging').fadeOut();
                }
                console.log("received-network dg_network_processJson");
                dg_network_processJson(data);
                console.log("received-network dg_network_selectPage");
                dg_network_selectPage(1);
                console.log("received-network dg_network_startForceLayout");
                dg_network_startForceLayout();
//                this.selectEntity(dg.network.data.json.center.key, false);
                this.deferAndTransition('viewing-network');
                this.selectEntity(dg.network.data.json.center.key, false);

            },
            'received-random': function(data) {
                this.requestNetwork(data.center, true);
            },
            'received-radial': function(data) {
                dg.relations.data = data;
                dg.relations.byYear = d3.group(data.results,
                                               function(d) { return d.year; },
                                               function(d) { return d.category; });
//                    .nest()
//                    .key(function(d) { return d.year; })
//                    .key(function(d) { return d.category; })
//                    .entries(data.results);
                dg.relations.byRole = d3.group(dg.relations.data.results, function(d) { return d.role; })
                    .groupSort(d3.ascending)
                    .rollup(function(leaves) { return leaves.length; });
//                    .nest()
//                    .key(function(d) { return d.role; })
//                    .sortKeys(d3.ascending)
//                    .rollup(function(leaves) { return leaves.length; })
//                    .entries(dg.relations.data.results);
                this.transition('viewing-radial');
            },
        },
    },
    handleError: function(error) {
        var message = 'Something went wrong!';
        var status = error.status;
        if (status == 0) {
            status = 404;
        } else if (status == 429) {
            message = 'Hey, slow down, buddy. Give it a minute.'
        }
        var text = [
            '<div class="alert alert-danger alert-dismissible" role="alert">',
            '<button type="button" class="close" data-dismiss="alert" aria-label="Close">',
            '<span aria-hidden="true">&times;</span>',
            '</button>',
            '<strong>' + status + '!</strong> ' + message,
            '</div>'
            ].join('');
        $('#flash').append(text);
        $('#filter select').val(this.rolesBackup).trigger('change');
        this.transition('viewing-network');
    },
    getNetworkURL: function(entityKey) {
        var entityType = entityKey.split('-')[0];
        var entityId = entityKey.split('-')[1];
        var url = '/api/' + entityType + '/network/' + entityId;
        var params = {'roles': $('#filter select').val()};
        if (params.roles) {
            url += '?' + decodeURIComponent($.param(params));
        }
        return url;
    },
    getRandomURL: function() {
        var url = '/api/random?r=' + Math.floor(Math.random() * 1000000);
        var params = {'roles': $('#filter select').val()};
        if (params.roles) {
            url += '&' + decodeURIComponent($.param(params));
        }
        return url;
    },
    getRadialURL: function(entityKey) {
        var entityType = entityKey.split("-")[0];
        var entityId = entityKey.split("-")[1];
        return '/api/' + entityType+ '/relations/' + entityId;
    },
    loadInlineData: function() {
        if (dgData) { this.handle('load-inline-data'); }
    },
    pushState: function(entityKey, params) {
        console.log("pushstate");
        var entityType = entityKey.split("-")[0];
        var entityId = entityKey.split("-")[1];
        var title = document.title;
        var url = "/" + entityType + "/" + entityId;
        if (params) {
            url += "?" + decodeURIComponent($.param(params));
        }
        var state = {key: entityKey, params: params};
        window.history.pushState(state, title, url);
        // ### TODO setup analytics ga('send', 'pageview', url);
        // ### TODO setup analytics ga('set', 'page', url);
    },
    requestNetwork: function(entityKey, pushHistory) {
        console.log("requestNetwork");
        this.transition('requesting');
        var self = this;
        d3.json(this.getNetworkURL(entityKey))
            .then(function(data) {
                self.handle('received-network', data, pushHistory);
            })
            .catch(function(error) {
                self.handleError(error);
            });
    },
    requestRadial: function(entityKey) {
        this.transition('requesting');
        var self = this;
        d3.json(this.getRadialURL(entityKey))
            .then(function(data) {
                self.handle('received-radial', data);
            })
            .catch(function(error) {
                self.handleError(error);
            });
    },
    requestRandom: function() {
        this.transition('requesting');
        var self = this;
        d3.json(this.getRandomURL())
            .then(function(data) {
                self.handle('received-random', data);
            })
            .catch(function(error) {
                self.handleError(error);
            });
    },
    selectEntity: function(entityKey, fixed) {
        this.handle('select-entity', entityKey, fixed);
    },
    selectNextPage: function() {
        var page = dg.network.pageData.currentPage + 1;
        if (dg.network.data.pageCount < page) {
            page = 1;
        }
        this.selectPage(page);
    },
    selectPreviousPage: function() {
        var page = dg.network.pageData.currentPage - 1;
        if (page == 0) {
            page = dg.network.data.pageCount;
        }
        this.selectPage(page);
    },
    selectPage: function(page) {
        console.log("selectPage");
        dg_network_selectPage(page);
        dg_network_startForceLayout();
        this.selectEntity(dg.network.pageData.selectedNodeKey, true);
    },
    showNetwork: function() {
        this.handle('show-network');
    },
    showRadial: function() {
        this.handle('show-radial');
    },
    toggleFilter: function(status) {
        if (status) {
            $('#filter-roles').attr('disabled', false);
        } else {
            $('#filter-roles').attr('disabled', true);
        }
    },
    toggleNetwork: function(status) {
        console.log("toggleNetwork: ", status);
        if (status) {
            if (1 < dg.network.data.json.pages) {
                $('#paging').fadeIn();
            } else {
                $('#paging').fadeOut();
            }
            console.log("dg.network.layers.root: ", dg.network.layers.root);
            dg.network.layers.root
                .transition()
                .duration(250)
                .style('opacity', 1);
//                .on('end', function(d, i) {
//                    dg.network.layers.link.selectAll('.link')
//                        .classed('noninteractive', false);
//                    dg.network.layers.node.selectAll('.node')
//                        .classed('noninteractive', false);
////                    console.log("toggleNetwork forceLayout start");
////                    dg.network.forceLayout.restart()
//                });
        } else {
            $('#paging').fadeOut();
            console.log("toggleNetwork: stop");
            dg.network.forceLayout.stop()
            dg.network.layers.root
                .transition()
                .duration(250)
                .style('opacity', 0.25);
//            dg.network.layers.link.selectAll('.link')
//                .classed('noninteractive', true);
//            dg.network.layers.node.selectAll('.node')
//                .classed('noninteractive', true);
        }
    },
    toggleLoading: function(status) {
        if (status) {
            var input = dg_loading_makeArray();
            var data = input[0], extent = input[1];
            $('#page-loading')
                .addClass('glyphicon-animate glyphicon-refresh');
        } else {
            var data = [], extent = [0, 0];
            $('#page-loading')
                .removeClass('glyphicon-animate glyphicon-refresh')
        }
        dg_loading_update(data, extent);
    },
    toggleRadial: function(status) {
        var self = this;
        if (status) {
            $('#entity-relations')
                .off('click')
                .on('click', function(event) { 
                    self.showNetwork();
                    event.preventDefault();
                });
            $('#entity-relations .glyphicon')
                .removeClass('glyphicon-eye-open')
                .addClass('glyphicon-eye-close');
        } else {
            $('#entity-relations')
                .off('click')
                .on('click', function(event) { 
                    self.showRadial();
                    event.preventDefault();
                });
            $('#entity-relations .glyphicon')
                .addClass('glyphicon-eye-open')
                .removeClass('glyphicon-eye-close');
        }
    },
});