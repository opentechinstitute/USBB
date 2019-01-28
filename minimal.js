//these three functions define the color schemes for the maps.
function getColor(cat) {
    if ((100 <= cat) && (cat < 1000)) {
        return '#034e7b';
    } else if ((50 <= cat) && (cat < 100)) {
        return '#0570b0';
    } else if ((25 <= cat) && (cat < 50)) {
        return '#3690c0';
    } else if ((10 <= cat) && (cat < 25)) {
        return '#74a9cf';
    } else if ((4 <= cat) && (cat < 10)) {
        return '#a6bddb';
    } else if ((.2 <= cat) && (cat < 4)) {
        return '#d0d1e6';
    } else if ((0 <= cat) && (cat < .2)) {
        return '#ece7f2';
    } else {
        '#fff';
    }
};

function getColorUp(cat) {
    if (5 <= cat) {
        return '#006d2c';
    } else if ((3.1 <= cat) && (cat < 5)) {
        return '#31a354';
    } else if ((2.9 <= cat) && (cat < 3.1)) {
        return '#74c476';
    } else if ((1 <= cat) && (cat < 2.9)) {
        return '#a1d99b';
    } else if ((.2 <= cat) && (cat < 1)) {
        return '#c7e9c0';
    } else if ((0 <= cat) && (cat < .2)) {
        return '#edf8e9';
    } else {
        '#fff';
    }
};

function getColorcomp(cat) {
    if (25 <= cat) {
        return '#01665e';
    } else if ((15 <= cat) && (cat < 25)) {
        return '#35978f';
    } else if ((5 <= cat) && (cat < 15)) {
        return '#80cdc1';
    } else if ((1 <= cat) && (cat < 5)) {
        return '#c7eae5';
    } else if ((-1 <= cat) && (cat < 1)) {
        return '#f5f5f5';
    } else if ((-5 <= cat) && (cat < -1)) {
        return '#f6e8c3';
    } else if ((-15 <= cat) && (cat < -5)) {
        return '#dfc27d';
    } else if ((-25 <= cat) && (cat < -15)) {
        return '#bf812d';
    } else if (cat < -25) {
        return '#8c510a';
    } else {
        '#fff000';
    }
};

function getColorcompperc(cat) {
    if (5 <= cat) {
        return '#01665e';
    } else if ((2.5 <= cat) && (cat < 5)) {
        return '#5ab4ac';
    } else if ((1.5 <= cat) && (cat < 2.5)) {
        return '#c7eae5';
    } else if ((.7 <= cat) && (cat < 1.5)) {
        return '#f5f5f5';
    } else if ((.5 <= cat) && (cat < .7)) {
        return '#f6e8c3';
    } else if ((.2 <= cat) && (cat < .5)) {
        return '#d8b365';
    } else if (cat < .2) {
        return '#8c510a';
    } else {
        '#fff';
    }
};

function getColorCounts(cat) {
    if (100000 <= cat) {
        return '#006d2c';
    } else if ((50000 <= cat) && (cat < 100000)) {
        return '#31a354';
    } else if ((15000 <= cat) && (cat < 50000)) {
        return '#74c476';
    } else if ((5000 <= cat) && (cat < 15000)) {
        return '#a1d99b';
    } else if ((1000 <= cat) && (cat < 5000)) {
        return '#c7e9c0';
    } else if ((0 <= cat) && (cat < 1000)) {
        return '#edf8e9';
    } else {
        '#fff';
    }
};

function getColorcutoff(cat) {
    if (cat == "Both below cutoffs") {
        return '#000004';
    } else if (cat == "Both above cutoffs") {
        return '#FCFDBF';
    } else if (cat == "Download below 10 Mbps") {
        return '#B63679';
    } else if (cat == "Upload below 1 Mbps") {
        return '#721F81';
    } else {
        '#fff';
    }
};


//simple helped functions for calculating maximums
function getMax(arr, prop) {
    var max;
    for (var i = 0; i < arr.length; i++) {
        if (!max || parseInt(arr[i][prop]) > parseInt(max[prop]))
            max = arr[i];
    }
    return max;
};
//creates the color legend html object and the stylization for the maps. it relies on colors specified in the variable definition code, not on the colors that are specified in the color scheme functions above.
//that means that when the scheme needs to change, it needs to be updated in both places
function createLegend(labels, colors, div_legend) {
    for (i = 0; i < labels.length; i++) {
        var label = labels[i];
        var color = colors[i];
        var item = document.createElement('div');
        var key = document.createElement('span');
        key.className = 'legend-key';
        key.style.backgroundColor = color;
        var value = document.createElement('span');
        value.innerHTML = label;
        item.appendChild(key);
        item.appendChild(value);
        div_legend.append(item);
    }
}
//creates the html object for the date slider and calls the function that gives that slider functionality. Could be updated to create sliders for other variables fairly simply.
function createSlider(location, slide_val) {
    var input_div = document.createElement('div');
    var h3 = document.createElement("h3");
    var label = document.createElement('label');
    var input = document.createElement('input');
    input_div.className = 'map-overlay-inner';
    h3.innerHTML = "FCC data range";
    label.id = "time_chunks";
    input.id = "slider";
    input.type = "range";
    input.min = 0;
    input.max = date_ids.length - 1;
    input.step = 1;
    input.value = slide_val;
    input_div.appendChild(h3);
    input_div.appendChild(label);
    input_div.appendChild(input);
    location.append(input_div);
    document.getElementById('time_chunks').textContent = date_names[slide_val];
    createSliderAction();
}

//adds functionality to an html slider. never called outside of "createSlider"
function createSliderAction() {
    document.getElementById('slider').addEventListener('input', function(time_chunk) {
        var cutoffs_obj_active = $("#broadband_cutoffs")[0].className; //when there are more standalones, change this to check the div for any active standalones
        var slid_ind = document.getElementById('slider').value;
        var geo = $("#geo_menu")[0].value;

        if (cutoffs_obj_active == "active") { //when there are more standalones, change this to check the div for any active standalones
            var keep_map = date_ids[slid_ind] + "_" + geo + "_" + "broadband_cutoffs";
        } else {
            var attribute = $("#attribute_menu")[0].value;
            var source = $("#source_menu")[0].value;
            var keep_map = date_ids[slid_ind] + "_" + geo + "_" + source + "_" + attribute;
        }

        document.getElementById('time_chunks').textContent = date_names[slid_ind];
        map.setLayoutProperty(keep_map + "_id", 'visibility', 'visible');
        turnOffOtherMaps(keep_map);
    });
}
//creates the data-driven colors of the map. It's pretty hard-coded so whenever new variable types to display are added this needs to be updated. It doesn't though, need to be updated when filters
//like date or region are added.
function createColorvector(data, data_name) {
    var expression_speed_mlab = ["match", ["get", data_name]];
    var expression_speed_mlab_up = ["match", ["get", data_name]];
    var expression_speed_477 = ["match", ["get", data_name]];
    var expression_speed_477_up = ["match", ["get", data_name]];
    var expression_speed_diff = ["match", ["get", data_name]];
    var expression_speed_diff_up = ["match", ["get", data_name]];
    var expression_speed_cutoffs = ["match", ["get", data_name]];
    data.forEach(function(row) {
        if (row[data_name] === undefined) {
            //do nothing
        } else {
            var color_mlab = getColor(row["speed_mlab"]);
            var color_mlab_up = getColorUp(row["speed_mlab_up"]);
            var color_477 = getColor(row["speed_477"]);
            var color_477_up = getColorUp(row["speed_477_up"]);
            var color_diff = getColorcomp(row["speed_diff"]);
            var color_diff_up = getColorcomp(row["speed_diff_up"]);
            var color_cutoffs = getColorcutoff(row["broadband_cutoffs"]);
            if (color_mlab == undefined) {
                color_mlab = "rgba(211,211,211,1)";
            } else {
                //don't change anything
            }
            if (color_mlab_up == undefined) {
                color_mlab_up = "rgba(211,211,211,1)";
            } else {
                //don't change anything
            }
            if (color_477 == undefined) {
                color_477 = "rgba(211,211,211,1)";
            } else {
                //don't change anything
            }
            if (color_477_up == undefined) {
                color_477_up = "rgba(211,211,211,1)";
            } else {
                //don't change anything
            }
            if (color_diff == undefined) {
                color_diff = "rgba(211,211,211,1)";
            } else {
                //don't change anything
            }
            if (color_diff_up == undefined) {
                color_diff_up = "rgba(211,211,211,1)";
            } else {
                //don't change anything
            }
            if (color_cutoffs == undefined) {
                color_cutoffs = "rgba(211,211,211,1)";
            } else {
                //don't change anything
            }


            expression_speed_mlab.push(row[data_name], color_mlab);
            expression_speed_mlab_up.push(row[data_name], color_mlab_up);
            expression_speed_477.push(row[data_name], color_477);
            expression_speed_477_up.push(row[data_name], color_477_up);
            expression_speed_diff.push(row[data_name], color_diff);
            expression_speed_diff_up.push(row[data_name], color_diff_up);
            expression_speed_cutoffs.push(row[data_name], color_cutoffs);
        }
    });
    expression_speed_mlab.push("rgba(211,211,211,1)");
    expression_speed_mlab_up.push("rgba(211,211,211,1)");
    expression_speed_477.push("rgba(211,211,211,1)");
    expression_speed_477_up.push("rgba(211,211,211,1)");
    expression_speed_diff.push("rgba(211,211,211,1)");
    expression_speed_diff_up.push("rgba(211,211,211,1)");
    expression_speed_cutoffs.push("rgba(211,211,211,1)");
    return ([
        [expression_speed_mlab, expression_speed_mlab_up, expression_speed_cutoffs],
        [expression_speed_477, expression_speed_477_up],
        [expression_speed_diff, expression_speed_diff_up]
    ])
}
//this function makes invisible data layers that aren't provided as input. That's the trick of how this app styles the map.
function turnOffOtherMaps(current_map) {

    //this layer stays on top of other layers so don't remove the other maps
    if (current_map.endsWith("speed_muni")) {

    } else {
        //turn off main maps
        for (var k = 0; k < date_ids.length; k++) {
            for (var i = 0; i < geo_ids.length; i++) {
                for (var j = 0; j < source_ids.length; j++) {
                    for (var l = 0; l < attribute_ids.length; l++) {
                        var map_name = date_ids[k] + "_" + geo_ids[i] + "_" + source_ids[j] + "_" + attribute_ids[l]
                        if (map_name != current_map) {
                            map.setLayoutProperty(map_name + "_id", 'visibility', 'none')
                        }
                    }
                }
            }
        }

        //turn off cutoff maps
        for (var k = 0; k < date_ids.length; k++) {
            for (var i = 0; i < geo_ids.length; i++) {
                var map_name = date_ids[k] + "_" + geo_ids[i] + "_broadband_cutoffs"
                if (map_name != current_map) {
                    map.setLayoutProperty(map_name + "_id", 'visibility', 'none')
                }

            }

        }
    }
}

//creates the html toggle bars that appear on the right of the map
function makeToggle(toggleIDs, toggleableLayerNames, active_level, id_name, make_legend) {
    var select = document.getElementById(id_name);
    for (var i = 0; i < toggleableLayerNames.length; i++) {
        var opt = toggleableLayerNames[i];
        var el = document.createElement("option");
        el.textContent = opt;
        el.value = toggleIDs[i];
        select.appendChild(el);
    }
    select.onchange = function() { createToggleMechanics(this, make_legend) };
}


//similar to the two functions used to create the slider, the html object and the mechanics of the object are created by two separate functions. This creates the mechanics
function createToggleMechanics(this_Toggle, make_legend) {
    var cutoffs_obj_active = $("#broadband_cutoffs")[0].className;
    var clickedLayer = this_Toggle.value;
    date_ind = document.getElementById('slider').value
    var date = date_ids[date_ind];
    var geo = $("#geo_menu")[0].value;
    var attribute = $("#attribute_menu")[0].value;
    var source = $("#source_menu")[0].value;

    if (cutoffs_obj_active == "active") { //when there are more standalones, change this to check the div for any active standalones
        var keep_map = date + "_" + geo + "_" + "broadband_cutoffs";
        if ($.inArray(clickedLayer, standalone_admissible_toggles["broadband_cutoffs"]) >-1) { //change the broadband_cutoffs to the name of the variable that has the active standalone
            //this changes which link is active
            turnOffOtherMaps(keep_map);
            //this makes the newly active layer visible
            map.setLayoutProperty(keep_map + "_id", 'visibility', 'visible');
            this_Toggle.className = 'active';
        } else {
            var keep_map = date + "_" + geo + "_" + source + "_" + attribute;
            var visibility = map.getLayoutProperty(keep_map + "_id", 'visibility');
            //this changes which link is active
            turnOffOtherMaps(keep_map);
            //this makes the newly active layer visible
            map.setLayoutProperty(keep_map + "_id", 'visibility', 'visible');
            this_Toggle.className = 'active';
            $("#broadband_cutoffs")[0].className="";
        }

    } else {
        var keep_map = date + "_" + geo + "_" + source + "_" + attribute;
        var visibility = map.getLayoutProperty(keep_map + "_id", 'visibility');
        //this changes which link is active
        turnOffOtherMaps(keep_map);
        //this makes the newly active layer visible
        map.setLayoutProperty(keep_map + "_id", 'visibility', 'visible');
        this_Toggle.className = 'active';
    }

    //set this link to active in the zoomed case
    if (make_legend == true) {
        //this is the legend section
        var legend_div = document.createElement("div");
        legend_div.className = "map-overlay legend" + " " + clickedLayer;
        legend_div.id = legend_grouping[clickedLayer];
        $(".map-overlay_1").append(legend_div);
        var curr_legend = $("." + clickedLayer);
        var slid_ind = document.getElementById('slider').value;
        var to_make_inactive_div = $('div.map-overlay')
            .empty();
        var to_make_inactive_div = $('div.map-overlay-inner')
            .empty();
        createLegend(eval(legend_dict[source + "_" + attribute] + "_" + "labels"), eval(legend_dict[source + "_" + attribute] + "_" + "colors"), curr_legend);
        $('div.map-overlay')
            .filter(":empty")
            .remove()
        $('div.map-overlay-inner')
            .filter(":empty")
            .remove()
        createSlider(map_div, slid_ind);
    }

};

function makeStandalone(_id, text, menu_name, action, make_legend) {
    var id = _id

    var link = document.createElement('a');
    link.href = '#';
    link.id = _id
    link.className = '';
    link.textContent = text;

    if (action == true) {
        link.onclick = function() { createStandaloneAction(this, _id, make_legend) };
    } else {
        link.onclick = function(e) {

            var clickedLayer = this.id;
            var visibility = map.getLayoutProperty(clickedLayer, 'visibility');

            if (visibility === 'visible') {
                map.setLayoutProperty(clickedLayer, 'visibility', 'none');
                this.className = '';
            } else {
                this.className = 'active';
                map.setLayoutProperty(clickedLayer, 'visibility', 'visible');
            }
        };
        map.setLayoutProperty(_id, 'visibility', 'none');
    }
    var layers = document.getElementById(menu_name);
    layers.appendChild(link);

}

function createStandaloneAction(this_Standalone, standalone_id, make_legend) {
    var clickedLayer = this_Standalone.id
    date_ind = document.getElementById('slider').value
    var date = date_ids[date_ind];
    var geo = $("#geo_menu")[0].value;
    var keep_map = date + "_" + geo + "_" + standalone_id;
    var visibility = map.getLayoutProperty(keep_map + "_id", 'visibility');

    if(this_Standalone.className=="active"){
        make_legend=false
    }

    //set this link to active in the zoomed case
    if (make_legend == true) {
        //this is the legend section
        var legend_div = document.createElement("div");
        legend_div.className = "map-overlay legend" + " " + clickedLayer;
        legend_div.id = legend_grouping[clickedLayer];
        $(".map-overlay_1").append(legend_div);
        var curr_legend = $("." + clickedLayer);
        var slid_ind = document.getElementById('slider').value;
        var to_make_inactive_div = $('div.map-overlay')
            .empty();
        var to_make_inactive_div = $('div.map-overlay-inner')
            .empty();
        createLegend(eval(legend_dict[clickedLayer] + "_" + "labels"), eval(legend_dict[clickedLayer] + "_" + "colors"), curr_legend);
        $('div.map-overlay')
            .filter(":empty")
            .remove()
        $('div.map-overlay-inner')
            .filter(":empty")
            .remove()
        createSlider(map_div, slid_ind);
    }
    //up until here
    //this changes which link is active
    turnOffOtherMaps(keep_map);

    //this makes the newly active layer visible
    for (var j = 0; j < geo_ids.length; j++) {
        map.setLayoutProperty(keep_map + "_id", 'visibility', 'visible');
    }

    this_Standalone.className = 'active';
}

function loadStandalone(standalone_id, source_ind, attribute_ind) {
    for (var k = 0; k < date_ids.length; k++) {
        for (var i = 0; i < geo_ids.length; i++) {
            if (geo_ids[i] == "state_house") {
                map.on('click', date_ids[k] + "_" + geo_ids[i] + "_" + standalone_id + "_id", function(e) {

                    new mapboxgl.Popup()
                        .setLngLat(e.lngLat)
                        .setHTML("<p>State House district:" + e.features[0].properties.house_num + "</p>")
                        .addTo(map);
                });
                map.addLayer({
                        "id": date_ids[k] + "_" + geo_ids[i] + "_" + standalone_id + "_id",
                        "type": "fill",
                        "source": "legislative_lower_state",
                        "source-layer": "full_speed_data_census_leg_lo-1ltr3a",
                        "visibility": "none",
                        "paint": {
                            "fill-color": colorArrayTime[k][i][source_ind][attribute_ind],
                            "fill-outline-color": "#cecece"
                        }
                    }, "water" //add a layer id here to insert this layer just before that one
                );
            } else if (geo_ids[i] == "state_senate") {
                map.on('click', date_ids[k] + "_" + geo_ids[i] + "_" + standalone_id + "_id", function(e) {
                    new mapboxgl.Popup()
                        .setLngLat(e.lngLat)
                        .setHTML("<p>State Senate district:" + e.features[0].properties.house_num + "</p>")
                        .addTo(map);
                });
                map.addLayer({
                        "id": date_ids[k] + "_" + geo_ids[i] + "_" + standalone_id + "_id",
                        "type": "fill",
                        "source": "legislative_upper_state",
                        "source-layer": "full_speed_data_census_leg_up-39wwkk",
                        "visibility": "none",
                        "paint": {
                            "fill-color": colorArrayTime[k][i][source_ind][attribute_ind],
                            "fill-outline-color": "#cecece"
                        }
                    }, "water" //add a layer id here to insert this layer just before that one
                );
            } else if (geo_ids[i] == "county") {
                map.on('click', date_ids[k] + "_" + geo_ids[i] + "_" + standalone_id + "_id", function(e) {
                    new mapboxgl.Popup()
                        .setLngLat(e.lngLat)
                        .setHTML("<p>County:" + e.features[0].properties.county + "</p>")
                        .addTo(map);
                });
                map.addLayer({
                        "id": date_ids[k] + "_" + geo_ids[i] + "_" + standalone_id + "_id",
                        "type": "fill",
                        "source": "county",
                        "source-layer": "full_speed_data_census_base_use",
                        "visibility": "none",
                        "paint": {
                            "fill-color": colorArrayTime[k][i][source_ind][attribute_ind],
                            "fill-outline-color": "#cecece"
                        }
                    }, "water" //add a layer id here to insert this layer just before that one
                );
            }
        }
    }
}
//This function is used to split up layer loading so that some amount of map shows up quickly while the others load
function loadLayers(time_lower, time_upper, geo_lower, geo_upper, source_lower, source_upper, attribute_lower, attribute_upper) {
    for (var k = time_lower; k < time_upper; k++) {
        for (var i = geo_lower; i < geo_upper; i++) {
            for (var j = source_lower; j < source_upper; j++) {
                for (var l = attribute_lower; l < attribute_upper; l++) {
                    if (geo_ids[i] == "state_house") {
                        map.on('click', date_ids[k] + "_" + geo_ids[i] + "_" + source_ids[j] + "_" + attribute_ids[l] + "_id", function(e) {

                            new mapboxgl.Popup()
                                .setLngLat(e.lngLat)
                                .setHTML("<p>State House district:" + e.features[0].properties.house_num + "</p>")
                                .addTo(map);
                        });
                        map.addLayer({
                                "id": date_ids[k] + "_" + geo_ids[i] + "_" + source_ids[j] + "_" + attribute_ids[l] + "_id",
                                "type": "fill",
                                "source": "legislative_lower_state",
                                "source-layer": "full_speed_data_census_leg_lo-1ltr3a",
                                "visibility": "none",
                                "paint": {
                                    "fill-color": colorArrayTime[k][i][j][l],
                                    "fill-outline-color": "#cecece"
                                }
                            }, "water" //add a layer id here to insert this layer just before that one
                        );
                    } else if (geo_ids[i] == "state_senate") {
                        map.on('click', date_ids[k] + "_" + geo_ids[i] + "_" + source_ids[j] + "_" + attribute_ids[l] + "_id", function(e) {
                            new mapboxgl.Popup()
                                .setLngLat(e.lngLat)
                                .setHTML("<p>State Senate district:" + e.features[0].properties.house_num + "</p>")
                                .addTo(map);
                        });
                        map.addLayer({
                                "id": date_ids[k] + "_" + geo_ids[i] + "_" + source_ids[j] + "_" + attribute_ids[l] + "_id",
                                "type": "fill",
                                "source": "legislative_upper_state",
                                "source-layer": "full_speed_data_census_leg_up-39wwkk",
                                "visibility": "none",
                                "paint": {
                                    "fill-color": colorArrayTime[k][i][j][l],
                                    "fill-outline-color": "#cecece"
                                }
                            }, "water" //add a layer id here to insert this layer just before that one
                        );
                    } else if (geo_ids[i] == "county") {
                        map.on('click', date_ids[k] + "_" + geo_ids[i] + "_" + source_ids[j] + "_" + attribute_ids[l] + "_id", function(e) {
                            new mapboxgl.Popup()
                                .setLngLat(e.lngLat)
                                .setHTML("<p>County:" + e.features[0].properties.county + "</p>")
                                .addTo(map);
                        });
                        map.addLayer({
                                "id": date_ids[k] + "_" + geo_ids[i] + "_" + source_ids[j] + "_" + attribute_ids[l] + "_id",
                                "type": "fill",
                                "source": "county",
                                "source-layer": "full_speed_data_census_base_use",
                                "visibility": "none",
                                "paint": {
                                    "fill-color": colorArrayTime[k][i][j][l],
                                    "fill-outline-color": "#cecece"
                                }
                            }, "water" //add a layer id here to insert this layer just before that one
                        );
                    }
                }
            }
        }
    }
};
