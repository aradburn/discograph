module.exports = function(grunt) {
    grunt.initConfig({
        pkg: grunt.file.readJSON('package.json'),
        less: {
            build: {
                options: {
                    //compress: true,
                    //yuicompress: true,
                    //optimization: 2
                },
                files: {
                    'static/css/discograph.css': 'source/css/discograph.less'
                },
            },
        },
        concat: {
            options: {
                // define a string to put between each file in the concatenated output
                separator: '',
                banner: '! function() { var dg = { version: "0.2" };',
                footer: 'if (typeof define === "function" && define.amd) define(dg); else if (typeof module === "object" && module.exports) module.exports = dg; this.dg = dg; }();',
                process: true,
            },
            dist: {
                // the files to concatenate
                src: [
                    'source/js/color.js',
                    'source/js/loading.js',
                    'source/js/network/*.js',
                    'source/js/svg.js',
                    'source/js/relations.js',
                    'source/js/typeahead.js',
                    'source/js/fsm.js',
                    'source/js/init.js',
                ],
                // the location of the resulting JS file
                dest: 'static/js/discograph.js',
            },

        },
        jsbeautifier: {
            files: ['static/js/discograph.js'],
            options: {
                braceStyle: 'collapse',
                breakChainedMethods: true,
                e4x: false,
                evalCode: false,
                indentChar: ' ',
                indentLevel: 4,
                indentSize: 2,
                indentWithTabs: false,
                jslintHappy: true,
                keepArrayIndentation: false,
                keepFunctionIndentation: false,
                maxPreserveNewlines: 0,
                preserveNewLines: false,
                spaceBeforeConditional: true,
                spaceInParen: true,
                unescapeStrings: false,
                wrapLineLength: 79,
                endWithNewline: true
            }
        },
        watch: {
            js: {
                files: ['source/js/**'],
                tasks: ['concat', 'jsbeautifier'],
                },
            css: {
                files: ['source/css/**'],
                tasks: ['less'],
                }
        }
    });
    grunt.loadNpmTasks("grunt-jsbeautifier");
    grunt.loadNpmTasks('grunt-contrib-concat');
    grunt.loadNpmTasks('grunt-contrib-less');
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.registerTask('default', ['concat', 'jsbeautifier', 'less', 'watch']);
};