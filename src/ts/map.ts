import * as mapboxgl from 'mapbox-gl';
import { LngLat, LngLatBounds } from "mapbox-gl";
import * as d3 from 'd3';
import * as geojson from 'geojson';
import $ from 'jquery';
import { globalData, downloadData, County, House, filteredData } from './index';
import { createSlider, makeToggle, createLegend, loadLayers, makeStandalone, loadStandalone, turnOffOtherMaps } from './minimal';

//map bounds
var bounds = new LngLatBounds(
    [
        new LngLat(-191.527239, -10), // Southwest coordinates
        new LngLat(2.565, 71.388889)  // Northeast coordinates
    ]);

(mapboxgl as any).accessToken = 'pk.eyJ1IjoibmV3YW1lcmljYSIsImEiOiIyM3ZnYUtrIn0.57fFgg_iM7S1wLH2GQC71g';
export var map = new mapboxgl.Map({
    container: 'map',
    style: 'mapbox://styles/newamerica/cjpn4e4df2gak2rp7nef3am21',
    center: [-99.9, 41.5],
    maxBounds: bounds,
    zoom: 3
});




//popup JSON
var geojson_markers = {
    type: 'FeatureCollection',
    features: [{
        type: 'Feature',
        geometry: {
            type: 'Point',
            coordinates: [-101.5649, 46.4178]
        },
        properties: {
            title: "North Dakota's Internet Exceptionalism",
            description: "The internet speeds in North Dakota look more like California's and New York's than the ones in neighboring Wyoming and Montana. <a href=\"https://nthieme.github.io/tangram-deserts/ND_blog.htm\" target=\"_blank\" title=\"Opens in a new window\">Click here to find out what makes them so different.</a>",
            icon: "library"
        }
    }, {
        type: 'Feature',
        geometry: {
            type: 'Point',
            coordinates: [-93.581543, 42.032974]
        },
        properties: {
            title: "The FCC says all of Iowa has access to broadband internet. Speed tests tell a different story.",
            description: "Using M-Lab data as a comparison point, Sam Bloch of The New Food Economy writes about <a href=\"https://newfoodeconomy.org/rural-iowa-broadband-data-fcc\" target=\"_blank\" title=\"Opens in a new window\"> the discrepancy between what the FCC says is happening in Iowa and what users are experiencing.</a>",
            icon: "library"
        }
    }, {
        type: 'Feature',
        geometry: {
            type: 'Point',
            coordinates: [-98.4842, 39.0119]
        },
        properties: {
            title: "Broadband Speed: FCC Map Vs. Experience on the Ground",
            description: "Looking at speeds in Kansas and Maine, Brian Whitacre, Sharon Strover, and Colin Rhinesmith critique the FCC's broadband map of the U.S., using M-Lab data <a href=\"https://www.dailyyonder.com/broadband-speed-fcc-map-vs-experience-ground/2018/07/25/26583\" target=\"_blank\" title=\"Opens in a new window\">as an alternative source of internet speeds.</a>",
            icon: "library"
        }
    }, {
        type: 'Feature',
        geometry: {
            type: 'Point',
            coordinates: [-66.5901, 19.4]
        },
        properties: {
            title: 'Puerto Rico’s Internet Problems Go from Bad to Worse',
            description: "In September 2017, Puerto Rico's internet speeds were at their peak, having finally improved after nearly a decade of stagnation. Then Hurricane Maria hit and speeds have yet to recover. <a href=\"https://www.pbs.org/wgbh/nova/article/puerto-rico-hurricane-maria-internet\" target=\"_blank\" title=\"Opens in a new window\">Here's the story of what happened with Puerto Rico's internet before and after Maria",
            icon: "library"
        }
    }]
};

var data_names = ["county", "house_num", "house_num"]; //this variable is needed because the name of the variable inside the array differs between the county and legis shapes
export var source_ids = ["MLab", "FCC", "Diff"]
export var geo_ids = ["county", "state_house", "state_senate"];
export var attribute_ids = ["DL", "UL"];
export var date_ids = ["dec_14", "jun_15", "dec_15", "jun_16", "dec_16", "jun_17"];

var geo_names = ["County", "State House", "State Senate"];
var source_names = ["M-Lab", "FCC", "Difference"]
export var date_names = ["Dec 2014", "Jun 2015", "Dec 2015", "Jun 2016", "Dec 2016", "Jun 2017"];
var attribute_names = ["Download", "Upload"]

export var legend_grouping = { "MLab_DL": "legend_mbps", "MLab_UL": "legend_mbps", "FCC_DL": "legend_mbps", "FCC_UL": "legend_mbps", "broadband_cutoffs": "legend_mbps" }; //need to add in others

export var legend_dict = { "MLab_DL": "DL", "MLab_UL": "UL", "FCC_DL": "DL", "FCC_UL": "UL", "Diff_DL": "Diff_DL", "Diff_UL": "Diff_UL", "broadband_cutoffs": "broadband_cutoffs" };
export var standalone_admissible_toggles = { "broadband_cutoffs": ["county", "state_senate", "state_house"] } //this is a nice way to enable and disable toggles for standalones

var DL_labels = ["Greater than 100 Mbps", "100 to 50 ", "25 to 50", "10 to 25", "4 to 10", "200 Kbps to 4 Mbps", "Less than 200 Kbps"];
var DL_colors = ['#034e7b', '#0570b0', '#3690c0', '#74a9cf', '#a6bddb', '#d0d1e6', '#ece7f2'];
var UL_labels = ["Greater than 5 Mbps", "3 to 5", "3 Mbps", "1 to 3", "200 Kbps to 1 Mbps", "Less than 200 Kbps"];
var UL_colors = ['#006d2c', '#31a354', '#74c476', '#a1d99b', '#c7e9c0', '#edf8e9'];
var Diff_DL_labels = ["Greater than 25", "15 to 25", "5 to 15", "1 to 5", "-1 to 1", "-5 to -1", "-15 to -5", "-25 to -15", "Less than -25"];
var Diff_DL_colors = ['#01665e', '#35978f', '#80cdc1', '#c7eae5', '#f5f5f5', '#f6e8c3', '#dfc27d', '#bf812d', '#8c510a'];
var Diff_P_DL_labels = ["Greater than 5x", "2.5x to 5x", "1.5x to 2.5x", ".7x to 1.5x", ".5x to .7x", ".2x to .5x", "Less than .2x"];
var Diff_P_DL_colors = ['#01665e', '#5ab4ac', '#c7eae5', '#f5f5f5', '#f6e8c3', '#d8b365', '#8c510a'];
var Diff_UL_labels = ["Greater than 25", "15 to 25", "5 to 15", "1 to 5", "-1 to 1", "-5 to -1", "-15 to -5", "-25 to -15", "Less than -25"];
var Diff_UL_colors = ['#01665e', '#35978f', '#80cdc1', '#c7eae5', '#f5f5f5', '#f6e8c3', '#dfc27d', '#bf812d', '#8c510a'];

var speed_muni_labels = ["Has municipal broadband"];
var speed_muni_colors = ["#d07386"];

var broadband_cutoffs_labels = ["Both above cutoffs", "Download below 10 Mbps", "Upload below 1 Mbps", "Both below cutoffs"]
var broadband_cutoffs_colors = ['#FCFDBF', '#B63679', "#721F81", "#000004"]

var center_array:[string, LngLat][] = [
        ["Alabama", new LngLat(-86.791130, 32.806671)],
        ["Alaska", new LngLat(-152.404419, 61.370716)],
        ["Arizona", new LngLat(-111.431221, 33.729759)],
        ["Arkansas", new LngLat(-92.373123, 34.969704)],
        ["California", new LngLat(-119.681564, 36.116203)],
        ["Colorado", new LngLat(-105.311104, 39.059811)],
        ["Connecticut", new LngLat(-72.755371, 41.597782)],
        ["Delaware", new LngLat(-75.507141, 39.318523)],
        ["District of Columbia", new LngLat(-77.026817, 38.897438)],
        ["Florida", new LngLat(-81.686783, 27.766279)],
        ["Georgia", new LngLat(-83.643074, 33.040619)],
        ["Hawaii", new LngLat(-157.498337, 21.094318)],
        ["Idaho", new LngLat(-114.478828, 44.240459)],
        ["Illinois", new LngLat(-88.986137, 40.349457)],
        ["Indiana", new LngLat(-86.258278, 39.849426)],
        ["Iowa", new LngLat(-93.210526, 42.011539)],
        ["Kansas", new LngLat(-96.726486, 38.526600)],
        ["Kentucky", new LngLat(-84.670067, 37.668140)],
        ["Louisiana", new LngLat(-91.867805, 31.169546)],
        ["Maine", new LngLat(-69.381927, 44.693947)],
        ["Maryland", new LngLat(-76.802101, 39.063946)],
        ["Massachusetts", new LngLat(-71.530106, 42.230171)],
        ["Michigan", new LngLat(-84.536095, 43.326618)],
        ["Minnesota", new LngLat(-93.900192, 45.694454)],
        ["Mississippi", new LngLat(-89.678696, 32.741646)],
        ["Missouri", new LngLat(-92.288368, 38.456085)],
        ["Montana", new LngLat(-110.454353, 46.921925)],
        ["Nebraska", new LngLat(-98.268082, 41.125370)],
        ["Nevada", new LngLat(-117.055374, 38.313515)],
        ["New Hampshire", new LngLat(-71.563896, 43.452492)],
        ["New Jersey", new LngLat(-74.521011, 40.298904)],
        ["New Mexico", new LngLat(-106.248482, 34.840515)],
        ["New York", new LngLat(-74.948051, 42.165726)],
        ["North Carolina", new LngLat(-79.806419, 35.630066)],
        ["North Dakota", new LngLat(-99.784012, 47.528912)],
        ["Ohio", new LngLat(-82.764915, 40.388783)],
        ["Oklahoma", new LngLat(-96.928917, 35.565342)],
        ["Oregon", new LngLat(-122.070938, 44.572021)],
        ["Pennsylvania", new LngLat(-77.209755, 40.590752)],
        ["Rhode Island", new LngLat(-71.511780, 41.680893)],
        ["South Carolina", new LngLat(-80.945007, 33.856892)],
        ["South Dakota", new LngLat(-99.438828, 44.299782)],
        ["Tennessee", new LngLat(-86.692345, 35.747845)],
        ["Texas", new LngLat(-97.563461, 31.054487)],
        ["Utah", new LngLat(-111.862434, 40.150032)],
        ["Vermont", new LngLat(-72.710686, 44.045876)],
        ["Virginia", new LngLat(-78.169968, 37.769337)],
        ["Washington", new LngLat(-121.490494, 47.400902)],
        ["West Virginia", new LngLat(-80.954453, 38.491226)],
        ["Wisconsin", new LngLat(-89.616508, 44.268543)],
        ["Wyoming", new LngLat(-107.302490, 42.755966)]
]

var states = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "District of Columbia", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"]



var select = document.getElementById("selectNumber");

for (var i = 0; i < states.length; i++) {
    var opt = states[i];
    var el = document.createElement("option");
    el.textContent = opt;
    el.value = opt;
    if (select != null) {
        select.appendChild(el);
    }
}

d3.select("#selectNumber").on("change", function() {
    var sel = document.getElementById('selectNumber') as HTMLSelectElement;
    if (sel != null) {
        var active_state = sel.options[sel.selectedIndex].value;
        var state_ind = states.indexOf(active_state)
        var fly_to_loc = center_array[state_ind][1]
        map.flyTo({
            center: fly_to_loc,
            zoom: 6,
            speed: .5
        });
    }

});

export var colorArrayTime = [];
var global_data = [];
var is_loaded = false;
var hasnt_run = true;
var zoomThreshold = 4;
var zoomed = false;
var active_date = 0;
var active_geo = "county";
var active_source = "MLab";
var active_attribute = "DL";
var starting_map = $(".legend");
var active_data_id = "speed_mlab"
starting_map.addClass(active_source);
export var map_div = $(".map-overlay_1");
createSlider(map_div, active_date);
makeToggle(source_ids, source_names, active_source, 'source_menu', true)
makeToggle(geo_ids, geo_names, active_geo, "geo_menu", false)
makeToggle(attribute_ids, attribute_names, active_attribute, "attribute_menu", true)
createLegend(eval(legend_dict[active_source + "_" + active_attribute] + "_" + "labels"), eval(legend_dict[active_source + "_" + active_attribute] + "_" + "colors"), starting_map);

//This section performs the initial load of data so that a map appears quicklymapbox_leg_county_counts_new_agg_up_full.json
$.getJSON('https://storage.googleapis.com/thieme-us-query/477/mapbox_final_json.json', function(data: downloadData) {
    var lower_json: House[] = [];
    var upper_json: House[] = [];
    for (var i = 0; i < data["house_"].length; i++) {
        if (data["house_"][i]["house"] == ["lower"]) {
            lower_json.push(data["house_"][i]);
        } else if (data["house_"][i]["house"] == ["upper"]) {
            upper_json.push(data["house_"][i]);
        }
    }
    let global_data: globalData = { "county": data["county"], "state_house": lower_json, "state_senate": upper_json }

    //data is the JSON string
    map.on('load', function() {
        map.addSource("legislative_lower_state", {
            type: "vector",
            url: "mapbox://newamerica.2b905y0r"
        });
        map.addSource("legislative_upper_state", {
            type: "vector",
            url: "mapbox://newamerica.7seqpql7"
        });
        map.addSource("county", {
            type: "vector",
            url: "mapbox://newamerica.biy2zq2m"
        });
        map.addSource("muni_zcta", {
            type: "vector",
            url: "mapbox://newamerica.10za116x"
        });

        var workerSends:{ [index: string] : number } = {};
        // Possible future enhancement: move the filtering in the below three lines into Workers as well
        //for (var j = 0; j < date_ids.length; j++) {
        for (let date of date_ids) {
            var filtered_house = global_data["state_house"].filter(function(entry) { return entry["date_range"] == [date]; });
            var filtered_senate = global_data["state_senate"].filter(function(entry) { return entry["date_range"] == [date]; });
            var filtered_county = global_data["county"].filter(function(entry) { return entry["date_range"] == [date]; });
            var data_filtered: filteredData = { "county": filtered_county, "state_house": filtered_house, "state_senate": filtered_senate }
            var colorArray = [];
            workerSends[date] = 0;
            for (var i = 0; i < geo_ids.length; i++) {
                var colorWorker = new Worker("worker.js");
                colorWorker.onmessage = function (e) {
                    colorArray[e.data.geo_id] = e.data.speed_data;
                    workerSends[date]--;
                    if (workerSends[e.data.date_id] === 0) {
                        colorArrayTime[e.data.date_id] = colorArray;
                    }
                    if (colorArrayTime.length === date_ids.length) {
                        // We've received back data from all of the workers. TODO: Is there a better way to figure this out?
                        loadLayers(0, 1, 0, 1, 0, 1, 0, 1);
                        is_loaded = true;
                    }
                };
                colorWorker.postMessage({
                    filtered: data_filtered[geo_ids[i]],
                    names: data_names[i],
                    date_id: date,
                    geo_id: i,
                });
                workerSends[date]++;
                //colorArray.push(createColorvector(data_filtered[geo_ids[i]], data_names[i]));
            }
            //colorArrayTime.push(colorArray)
        }

        
    }); // closes map load

}); //closes JSON load

// This section runs once the inital map has loaded and loads the rest of the data.
map.on("render", function() {
    if (map.loaded()) {
        if (is_loaded == true) {
            if (hasnt_run == true) {
                hasnt_run = false;

                loadLayers(0, date_ids.length, 0, geo_ids.length, 0, source_ids.length, 0, attribute_ids.length);

                //adding in standalone maps that don't fit into the above

                //Broadband cutoffs
                loadStandalone("broadband_cutoffs", 0, 2) //0 for MLab because it comes first in the colorArray at depth 2, 2 for Cutoff because it comes third in the MLab array at depth 3
                makeStandalone("broadband_cutoffs", "Broadband Cutoffs", "cutoff_menu", true, true)

                //Muni layer
                map.addLayer({
                    "id": "muni_networks",
                    "type": "fill",
                    "source": "muni_zcta",
                    "source-layer": "muni_broadband",
                    "paint": {
                        "fill-color": "#d07386",
                        "fill-outline-color": "#cecece"
                    }
                }, "border-admin-3-4-case");

                makeStandalone("muni_networks", "Municipal Broadband", "muni_menu", false, false)

                turnOffOtherMaps(date_ids[0] + "_" + geo_ids[0] + "_" + source_ids[0] + "_" + attribute_ids[0]);

                //add story popups
                //make story popups work
                // TODO: Fix the explicit any here
                map.on('click', 'places', function(e: any) {
                    var coordinates = e.features[0].geometry.coordinates.slice();
                    var description = e.features[0].properties.description;
                    // Ensure that if the map is zoomed out such that multiple
                    // copies of the feature are visible, the popup appears
                    // over the copy being pointed to.
                    while (Math.abs(e.lngLat.lng - coordinates[0]) > 180) {
                        coordinates[0] += e.lngLat.lng > coordinates[0] ? 360 : -360;
                    }
                    new mapboxgl.Popup()
                        .setLngLat(coordinates)
                        .setHTML(description)
                        .addTo(map);
                });
                // Change the cursor to a pointer when the mouse is over the places layer.
                map.on('mouseenter', 'places', function() {
                    map.getCanvas().style.cursor = 'pointer';
                });
                // Change it back to a pointer when it leaves.
                map.on('mouseleave', 'places', function() {
                    map.getCanvas().style.cursor = '';
                });

                geojson_markers.features.forEach(function(marker) {
                    // create a HTML element for each feature
                    var el = document.createElement('div');
                    el.className = 'marker';
                    // make a marker for each feature and add to the map
                    new mapboxgl.Marker(el)
                        .setLngLat(new LngLat(marker.geometry.coordinates[0],marker.geometry.coordinates[1]))
                        .setPopup(new mapboxgl.Popup({ offset: 25 }) // add popups
                            .setHTML('<h3>' + marker.properties.title + '</h3><p>' + marker.properties.description + '</p>'))
                        .addTo(map);
                });
            } // hasnt run
        } // is loaded 
    } // loaded
}); //render
