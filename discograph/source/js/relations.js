/**
 * @fileoverview Relations visualization module for Discograph
 * This module handles the creation and management of radial/circular relationship visualizations
 * using D3.js. It provides functionality for creating interactive circular charts that display
 * relationships between different roles or entities.
 */

import * as d3 from 'd3';
import { dg } from './dg';

/**
 * Initializes the relations visualization by setting up the base SVG container
 * Creates a root group element for all relation-based visualizations
 */
export function initRelations() {
    const svgElement = d3.select("#svg");
    const root = svgElement.append("g").attr("id", "relationsLayer");
    dg.relations.layers.root = root;

    // Zoom functionality commented out for now
    // relations.zoom = d3.zoom()
    //     .extent([[0, 0], [dg.svg_dimensions[0], dg.svg_dimensions[1]]])
    //     .scaleExtent([1, 8])
    //     .on("zoom", handleZoom);
    // svgElement.call(relations.zoom)
}

/**
 * Sets the relations data
 * @param {Object} data - The relations data to set
 */
export function setRelationsData(data) {
    dg.relations.data = data;
                
    // Group data by year and category
    dg.relations.byYear = d3.group(data.results,
        function(d) { return d.year; },
        function(d) { return d.category;}
    );

    //                    .nest()
    //                    .key(function(d) { return d.year; })
    //                    .key(function(d) { return d.category; })
    //                    .entries(data.results);
    //                gByRole = d3.group(data.results, function(d) { return d.role; })
    //                console.log("REQUESTING gByRole: ", gByRole);

    // Process role data
    const sortedByRole = d3.sort(data.results, function(d) { return d.role; });
    dg.relations.byRole = d3.rollup(sortedByRole, function(d) { return d.length; }, function(d) { return d.role; });
    console.log("dg.relations.byRole: ", dg.relations.byRole);

    //                dg.relations.byRole = d3.group(data.results, function(d) { return d.role; })
    //                                        .rollup(function(d) {
    //                                            console.log("REQUESTING d: ", d);
    //                                            return d.length;
    //                                        });
    //                    .nest()
    //                    .key(function(d) { return d.role; })
    //                    .sortKeys(d3.ascending)
    //                    .rollup(function(leaves) { return leaves.length; })
    //                    .entries(dg.relations.data.results);
}

/**
 * Creates a radial chart visualization
 * This function handles the creation of a circular/radial chart that displays data
 * in a circular arrangement with segments sized according to their values
 * 
 * Features:
 * - Dynamic segment sizing based on data values
 * - Animated transitions for segment creation
 * - Interactive segments with hover effects
 * - Text labels for each segment (both inner and outer)
 */
export function createRadialChart() {
    console.log("createRadialChart()");
    
    const textAnchor = (d, i) => {
        const angle = (i + 0.5) / numBars;
        return angle < 0.5 ? 'start' : 'end';
    };

    // @ts-ignore
    const barHeight = d3.min(dg.dimensions) / 3;
    console.log("createRadialChart() barHeight:", barHeight);
    const data = dg.relations.byRole;
    console.log("createRadialChart() data: ", data);

    const extent = d3.extent(data, d => d[1]);
    console.log("createRadialChart() extent: ", extent);

    // @ts-ignore
    const barScale = d3.scaleSqrt(extent, [barHeight / 4, barHeight])
        .exponent(0.25);
    const numBars = data.size;

    const transform = (d, i) => {
        console.log("d: ", d);
        console.log("i: ", i);
        const hypotenuse = barScale(d[1]) + 5;
        const angle = (i + 0.5) / numBars;
        let degrees = (angle * 360);
        if (180 <= degrees) {
            degrees -= 180;
        }
        degrees -= 90;
        const radians = angle * 2 * Math.PI;
        const x = Math.sin(radians) * hypotenuse;
        const y = -Math.cos(radians) * hypotenuse;
        return [
            `rotate(${degrees},${x},${y})`,
            `translate(${x},${y})`
        ].join(' ');
    };

    initRelations();

    const arc = d3.arc()
        .startAngle((d, i) => (i * 2 * Math.PI) / numBars)
        .endAngle((d, i) => ((i + 1) * 2 * Math.PI) / numBars)
        .innerRadius(0);
    console.log("createRadialChart() arc: ", arc);

    dg.arc = arc;
    // @ts-ignore
    const radialGroup = relations.layers.root.append('g')
        .attr('class', 'radial centered')
        .attr('transform', `translate(${dg.dimensions[0] / 2},${dg.dimensions[1] / 2})`);

    const segments = radialGroup.selectAll('g')
        .data(data)
        .enter()
        .append('g')
        .attr('class', 'segment')
        .on('mouseover', function(event) { 
            d3.select(this).raise(); 
        });

    console.log("createRadialChart() segments: ", segments);
    
    const arcs = segments.append('path')
        .attr('class', 'arc')
        .attr('d', arc)
        .each(function(d) { 
            d.outerRadius = 0; 
        })
        .transition()
        .ease(d3.easeElastic)
        .duration(500)
        .delay((d, i) => (numBars - i) * 25)
        // @ts-ignore
        .attrTween('d', function(d, i) {
            console.log("attrTween d: ", d);
            const outer = d3.interpolate(d.outerRadius, barScale(d[1]));
            return function(t) {
                console.log("attrTween t: ", t);
                d.outerRadius = outer(t);
                const result = arc(d, i);
                console.log("attrTween result: ", result);
                return result;
            };
        });

    console.log("createRadialChart() arcs: ", arcs);

    segments.append('text')
        .attr('class', 'outer')
        .attr('text-anchor', textAnchor)
        .attr('transform', transform)
        .text(d => d[0]);

    segments.append('text')
        .attr('class', 'inner')
        .attr('text-anchor', textAnchor)
        .attr('transform', transform)
        .text(d => d[0]);

    console.log("createRadialChart() end");
}

/**
 * Handles zoom events for the relations visualization
 * Applies zoom transformations to the root layer of the visualization
 * 
 * @param {Object} param0 - Zoom event parameters
 * @param {d3.ZoomTransform} param0.transform - D3 zoom transform object
 */
export function handleZoom({ transform }) {
    // @ts-ignore
    relations.layers.root.attr("transform", transform);
}

/**
 * Clears the relations layer from the SVG
 * Removes the relations layer element from the SVG
 */
export function clearRelationsLayer() {
    d3.select('#relationsLayer').remove();
}