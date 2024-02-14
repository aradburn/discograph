function dg_svg_init() {
    // Setup window dimensions on SVG element
    dg_svg_set_size();

    // Setup SVG common definitions
    dg_svg_setupDefs();

    // Initialise tooltip
    d3.select('#svg')
        .call(nodeToolTip)
        .call(linkToolTip);
}

function dg_svg_set_size() {
    // Setup window dimensions on SVG element
    d3.select("#svg")
        .attr("width", dg.dimensions[0])
        .attr("height", dg.dimensions[1])
        .attr("viewBox", "0 0 " + dg.svg_dimensions[0] + " " + dg.svg_dimensions[1])
        .attr("preserveAspectRatio", "none");
}

function dg_svg_setupDefs() {
    var defs = d3.select("#svg").append("defs");
    // ARROWHEAD
    defs.append("marker")
        .attr("id", "arrowhead")
        .attr("viewBox", "-5 -5 10 10")
        .attr("refX", 4)
        .attr("refY", 0)
        .attr("markerWidth", 5)
        .attr("markerHeight", 5)
        .attr("markerUnits", "strokeWidth")
        .attr("orient", "auto")
        .append("path")
        .attr("d", "M 0,0 m -5,-5 L 5,0 L -5,5 L -2.5,0 L -5,-5 Z")
        .attr("stroke-linecap", "round")
        .attr("stroke-linejoin", "round");
    // AGGREGATE
    defs.append("marker")
        .attr("id", "aggregate")
        .attr("viewBox", "-5 -5 10 10")
        .attr("refX", 5)
        .attr("refY", 0)
        .attr("markerWidth", 5)
        .attr("markerHeight", 5)
        .attr("markerUnits", "strokeWidth")
        .attr("orient", "auto")
        .append("path")
        .attr("d", "M 0,0 m 5,0 L 0,-3 L -5,0 L 0,3 L 5,0 Z")
        .attr("fill", "#fff")
        .attr("stroke", "#000")
        .attr("stroke-linecap", "round")
        .attr("stroke-linejoin", "round")
        .attr("stroke-width", 1.5);
    // RADIAL GRADIENT
    var gradient = defs.append('radialGradient')
        .attr('id', 'radial-gradient');
    gradient.append('stop')
        .attr('offset', '0%')
        .attr('stop-color', '#333')
        .attr('stop-opacity', '1.0');
    gradient.append('stop')
        .attr('offset', '50%')
        .attr('stop-color', '#333')
        .attr('stop-opacity', '0.333');
    gradient.append('stop')
        .attr('offset', '75%')
        .attr('stop-color', '#333')
        .attr('stop-opacity', '0.111');
    gradient.append('stop')
        .attr('offset', '100%')
        .attr('stop-color', '#333')
        .attr('stop-opacity', '0.0');
}

function dg_svg_print(width, height) {
    dg_show_message("info", "Saving image to disk, please wait...");
    var svgNode = d3.select("#svg").node();

    var svgString = dg_svg_getSVGString(svgNode);
	dg_svg_string2Image(svgString, 2 * width, 2 * height, 'png', saveBlob); // passes Blob and filesize String to the callback

	function saveBlob( dataBlob, filesize ){
	    // Call FileSaver.js function
	    var entityKey = dg.network.pageData.selectedNodeKey;
	    var node = dg.network.data.nodeMap.get(entityKey);
		saveAs(dataBlob, "Discograph2 - " + node.name + ".png");

		dg_clear_messages(10);
		dg_show_message("success", "Saving image complete");
		dg_clear_messages(10000);
	}
}

function dg_svg_getSVGString(svgNode) {
	svgNode.setAttribute('xlink', 'http://www.w3.org/1999/xlink');
	var cssStyleText = getCSSStyles(svgNode);
	appendCSS(cssStyleText, svgNode);

	var serializer = new XMLSerializer();
	var svgString = serializer.serializeToString(svgNode);
	svgString = svgString.replace(/(\w+)?:?xlink=/g, 'xmlns:xlink='); // Fix root xlink without namespace
	svgString = svgString.replace(/NS\d+:href/g, 'xlink:href'); // Safari NS namespace fix

	return svgString;

	function getCSSStyles(parentElement) {
		var selectorTextArr = [];

		// Add Parent element Id and Classes to the list
		selectorTextArr.push('#' + parentElement.id);
		for (var c = 0; c < parentElement.classList.length; c++)
				if ( !contains('.' + parentElement.classList[c], selectorTextArr) )
					selectorTextArr.push('.' + parentElement.classList[c]);
		// Add Children element Ids and Classes to the list
		var nodes = parentElement.getElementsByTagName("*");
		for (var i = 0; i < nodes.length; i++) {
			var id = nodes[i].id;
			if ( !contains('#' + id, selectorTextArr) )
				selectorTextArr.push('#' + id);

			var classes = nodes[i].classList;

            // .nodeClass
			for (var c = 0; c < classes.length; c++) {
			    var selector = '.' + classes[c];
				if (!contains(selector, selectorTextArr)) {
					selectorTextArr.push(selector);
			    }
			    // nodeName.nodeClass
			    if (nodes[i].nodeName) {
				    var selector = nodes[i].nodeName + '.' + classes[c];
					if (!contains(selector, selectorTextArr)) {
				        selectorTextArr.push(selector);
			        }
                }
            }

            // #parent .nodeClass
			for (var c = 0; c < classes.length; c++) {
			    var parentId = nodes[i].parentNode.id;
			    var selector = '#' + parentId + ' .' + classes[c];
				if (parentId && !contains(selector, selectorTextArr) ) {
					selectorTextArr.push(selector);
				}
				// #parent nodeName.nodeClass
			    if (nodes[i].nodeName) {
				    var selector = '#' + parentId + " " + nodes[i].nodeName + '.' + classes[c];
					if (!contains(selector, selectorTextArr)) {
				        selectorTextArr.push(selector);
			        }
                }
			}

			// #parent's parent .nodeClass
			for (var c = 0; c < classes.length; c++) {
			    var parentNode = nodes[i].parentNode
			    if (parentNode) {
			        var parentId = parentNode.parentNode.id;
			        var selector = '#' + parentId + ' .' + classes[c];
    				if (parentId && !contains(selector, selectorTextArr) ) {
	    				selectorTextArr.push(selector);
			    	}
			    	if (nodes[i].nodeName) {
				        var selector = '#' + parentId + " " + nodes[i].nodeName + '.' + classes[c];
					    if (!contains(selector, selectorTextArr)) {
				            selectorTextArr.push(selector);
			            }
                    }
			    }
			}

			for (var c = 0; c < classes.length; c++) {
    			var parentNode = nodes[i].parentNode
			    var parentId = parentNode.id;
			    if (parentNode) {
			        var parentClasses = parentNode.classList;
			        var parentParentId = parentNode.parentNode.id;

			        for (var pc = 0; pc < parentClasses.length; pc++) {
			            // No nodeClass
			            var selector = '.' + parentClasses[pc];
        				if (!contains(selector, selectorTextArr)) {
		        			selectorTextArr.push(selector);
			            }
         			    // No nodeClass + nodeName
		        	    if (nodes[i].nodeName) {
				            var selector = '.' + parentClasses[pc] + ' ' + nodes[i].nodeName;
					        if (!contains(selector, selectorTextArr)) {
				                selectorTextArr.push(selector);
			                }
			            }

			            // Parent class + nodeClass
			            var selector = '.' + parentClasses[pc] + ' .' + classes[c];
        				if (!contains(selector, selectorTextArr)) {
		        			selectorTextArr.push(selector);
			            }
         			    // Parent class + nodeName.nodeClass
		        	    if (nodes[i].nodeName) {
				            var selector = '.' + parentClasses[pc] + ' ' + nodes[i].nodeName + '.' + classes[c];
					        if (!contains(selector, selectorTextArr)) {
				                selectorTextArr.push(selector);
			                }
			            }

                        // ParentParentID + Parent class
			            selector = '#' + parentParentId + ' .' + parentClasses[pc];
    				    if (parentParentId && !contains(selector, selectorTextArr) ) {
	    				    selectorTextArr.push(selector);
			    	    }
			    	    // ParentParentID + Parent class + nodeName
			            if (parentParentId && nodes[i].nodeName) {
				            var selector = '#' + parentParentId + ' .' + parentClasses[pc] + ' ' + nodes[i].nodeName;
					        if (!contains(selector, selectorTextArr)) {
				                selectorTextArr.push(selector);
			                }
                        }

                        // ParentParentID + Parent class + nodeClass
			            selector = '#' + parentParentId + ' .' + parentClasses[pc] + ' .' + classes[c];
    				    if (parentParentId && !contains(selector, selectorTextArr) ) {
	    				    selectorTextArr.push(selector);
			    	    }
			    	    // ParentParentID + Parent class + nodeName.nodeClass
			            if (parentParentId && nodes[i].nodeName) {
				            var selector = '#' + parentParentId + ' .' + parentClasses[pc] + ' ' + nodes[i].nodeName + '.' + classes[c];
					        if (!contains(selector, selectorTextArr)) {
				                selectorTextArr.push(selector);
			                }
                        }

                        // ParentParentParentID + Parent class
			            if (parentNode.parentNode) {
                            var parentParentParentId = parentNode.parentNode.parentNode.id;
                            selector = '#' + parentParentParentId + ' .' + parentClasses[pc];
                            if (parentParentParentId && !contains(selector, selectorTextArr) ) {
                                selectorTextArr.push(selector);
                            }
                            if (parentParentParentId && nodes[i].nodeName) {
                                var selector = '#' + parentParentParentId + ' .' + parentClasses[pc] + ' ' + nodes[i].nodeName;
                                if (!contains(selector, selectorTextArr)) {
                                    selectorTextArr.push(selector);
                                }
                            }
                        }

                        // ParentParentParentID + Parent class + nodeClass
			            if (parentNode.parentNode) {
                            var parentParentParentId = parentNode.parentNode.parentNode.id;
                            selector = '#' + parentParentParentId + ' .' + parentClasses[pc] + ' .' + classes[c];
                            if (parentParentParentId && !contains(selector, selectorTextArr) ) {
                                selectorTextArr.push(selector);
                            }
                            if (parentParentParentId && nodes[i].nodeName) {
                                var selector = '#' + parentParentParentId + ' .' + parentClasses[pc] + ' ' + nodes[i].nodeName + '.' + classes[c];
                                if (!contains(selector, selectorTextArr)) {
                                    selectorTextArr.push(selector);
                                }
                            }
                        }
			        }
                }
            }
		}

		// Extract CSS Rules
		var extractedCSSText = "";
		for (var i = 0; i < document.styleSheets.length; i++) {
			var s = document.styleSheets[i];

			try {
			    if (!s.cssRules)
			        continue;
			} catch(e) {
		    		if (e.name !== 'SecurityError') throw e; // for Firefox
		    		continue;
		    }

			var cssRules = s.cssRules;

			for (var r = 0; r < cssRules.length; r++) {
				if (contains(cssRules[r].selectorText, selectorTextArr)) {
					extractedCSSText += cssRules[r].cssText + "\n";
			    }
			}
		}

		return extractedCSSText;

		function contains(str, arr) {
			return arr.indexOf(str) === -1 ? false : true;
		}
	}

	function appendCSS(cssText, element) {
		var styleElement = document.createElement("style");
		styleElement.setAttribute("type","text/css");
		styleElement.innerHTML = cssText;
		var refNode = element.hasChildNodes() ? element.children[0] : null;
		element.insertBefore(styleElement, refNode);
	}
}

function dg_svg_string2Image(svgString, width, height, format, callback) {
	var format = format ? format : 'png';

    // Convert SVG string to data URL
	var imgsrc = 'data:image/svg+xml;base64,'+ btoa(unescape(encodeURIComponent(svgString)));

	var canvas = document.createElement("canvas");
	var context = canvas.getContext("2d");

	canvas.width = width;
	canvas.height = height;

	var image = new Image();
	image.onload = function() {
		context.clearRect (0, 0, width, height);
		context.drawImage(image, 0, 0, width, height);

		canvas.toBlob(function(blob) {
			var filesize = Math.round(blob.length/1024) + ' KB';
			if (callback) callback(blob, filesize);
		});
	};

	image.src = imgsrc;
}