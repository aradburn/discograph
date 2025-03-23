import * as d3 from 'd3';

/**
 * Class representing a loading visualization using D3.js
 */
export class Loading {
    constructor() {
        this.arc = null;
        this.barHeight = 200;
        this.layer = null;
        this.selection = null;
    }

    /**
     * Initializes the loading animation by creating an SVG layer and setting up base configurations
     * @param {Array} svgDimensions - Array containing [width, height] of the SVG
     */
    init(svgDimensions) {
        const layer = d3.select('#svg')
            .append('g')
            .attr('id', 'loadingLayer')
            .attr('class', 'centered')
            .attr('transform', `translate(${svgDimensions[0] / 2},${svgDimensions[1] / 2})`);

        this.arc = d3.arc()
            .startAngle(d => d.startAngle)
            .endAngle(d => d.endAngle)
            .innerRadius(d => d.innerRadius)
            .outerRadius(d => d.outerRadius);

        this.barHeight = 200;
        this.layer = layer;
        this.selection = layer.selectAll('path');
    }

    /**
     * Generates random data for the loading animation arcs
     * @returns {[Array, Array]} [data, extent] - Array containing arc data objects and their value extents
     */
    makeArray() {
        const count = 10;
        const values = [];
        const data = [];

        for (let i = 0; i < count; i++) {
            const pair = [Math.random(), Math.random()].sort();
            values.push(pair[0], pair[1]);
            
            data.push({
                active: true,
                startAngle: 2 * Math.PI * Math.random(),
                endAngle: 2 * Math.PI * Math.random(),
                rotationRate: Math.random() * 10,
                targetInnerRadius: pair[0],
                targetOuterRadius: pair[1],
            });
        }
        return [data, d3.extent(values)];
    }

    /**
     * Toggles the visibility of the loading animation
     * @param {boolean} status - Whether to show (true) or hide (false) the loading animation
     */
    toggle(status) {
        const [data, extent] = status ? this.makeArray() : [[], [0, 0]];
        const pageLoadingElement = document.getElementById('page-loading');
        if (pageLoadingElement) {
            pageLoadingElement.style.display = status ? 'block' : 'none';
        }
        this.update(data, extent);
    }

    /**
     * Updates the loading animation with new data
     * @param {Array} data - Array of arc data objects
     * @param {Array} extent - Min/max values for scaling
     */
    update(data, extent) {
        if (!this.layer) {
            console.error("Layer is not initialized.");
            return;
        }

        const barScale = d3.scaleLinear()
            .domain(extent)
            .range([this.barHeight / 4, this.barHeight]);

        const dataSelection = this.layer.selectAll('path')
            .data(data);

        const selectionEnter = dataSelection.enter();
        this.transitionEnter(selectionEnter);
        this.transitionExit(dataSelection.exit());

        const updatedSelection = this.layer.selectAll('path')
            .data(data);
            
        this.transitionUpdate(updatedSelection, barScale);
        
        if (selectionEnter.size() > 0) {
            this.rotate(updatedSelection);
        }
    }

    /**
     * Handles the entry transition for new arcs
     * @param {d3.Selection} selection - D3 selection of entering elements
     */
    transitionEnter(selection) {
        const scale = d3.scaleOrdinal(d3.schemeCategory10);
        
        selection
            .append('path')
            .attr('class', 'arc')
            .attr('d', this.arc)
            .attr('fill', (_, i) => scale(i.toString()))
            .each(d => {
                d.innerRadius = 0;
                d.outerRadius = 0;
                d.hasTimer = false;
            });
    }

    /**
     * Animates transitions for updating arcs
     * @param {d3.Selection} selection - D3 selection to update
     * @param {d3.ScaleLinear} barScale - Scale function for arc sizes
     */
    transitionUpdate(selection, barScale) {
        selection
            .transition()
            .duration(1000)
            .delay((_, i) => (selection.size() - i) * 100)
            // @ts-ignore
            .attrTween('d', (d) => {
                const inner = d3.interpolate(d.innerRadius, barScale(d.targetInnerRadius));
                const outer = d3.interpolate(d.outerRadius, barScale(d.targetOuterRadius));
                return (t) => {
                    d.innerRadius = inner(t);
                    d.outerRadius = outer(t);   
                    // @ts-ignore
                    return this.arc(d);
                };
            });
    }

    /**
     * Handles the exit transition for removing arcs
     * @param {d3.Selection} selection - D3 selection of elements to remove
     */
    transitionExit(selection) {
        return selection
            .transition()
            .duration(1000)
            .delay((_, i) => (selection.size() - i) * 100)
            // @ts-ignore
            .attrTween('d', (d) => {
                const inner = d3.interpolate(d.innerRadius, 0);
                const outer = d3.interpolate(d.outerRadius, 0);
                return (t) => {
                    d.innerRadius = inner(t);
                    d.outerRadius = outer(t);
                    // @ts-ignore
                    return this.arc(d);
                };
            })
            .on('end', function(d) {
                d.active = false;
                this.remove();
            });
    }

    /**
     * Manages continuous rotation animation of arcs
     * @param {d3.Selection} selection - D3 selection of elements to rotate
     */
    rotate(selection) {
        selection.each((d) => {
            if (d.hasTimer) return;
            
            d.hasTimer = true;
            d.timer = d3.interval((elapsed) => {
                if (!d.active) {
                    console.log("stop timer");
                    d.timer.stop();
                    d.hasTimer = false;
                    d.timer = null;
                }
                
                selection.attr('transform', () => {
                    let angle = elapsed * d.rotationRate;
                    if (0 < d.outerRadius) {
                        angle = angle / d.outerRadius;
                    }
                    return `rotate(${angle})`;
                });
            }, 20);
        });
    }
}

// Export a singleton instance
export const loading = new Loading();