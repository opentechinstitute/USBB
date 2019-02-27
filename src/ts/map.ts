import { LngLat, LngLatBounds } from "mapbox-gl";
import { selection } from 'd3-selection';

//map bounds
var bounds = new LngLatBounds(
    [
        new LngLat(-191.527239, -10), // Southwest coordinates
        new LngLat(2.565, 71.388889)  // Northeast coordinates
    ]);

mapboxgl.accessToken = 'pk.eyJ1IjoibmV3YW1lcmljYSIsImEiOiIyM3ZnYUtrIn0.57fFgg_iM7S1wLH2GQC71g';
var map = new mapboxgl.Map({
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
var source_ids = ["MLab", "FCC", "Diff"]
var geo_ids = ["county", "state_house", "state_senate"];
var attribute_ids = ["DL", "UL"];
var date_ids = ["dec_14", "jun_15", "dec_15", "jun_16", "dec_16", "jun_17"];

var geo_names = ["County", "State House", "State Senate"];
var source_names = ["M-Lab", "FCC", "Difference"]
var date_names = ["Dec 2014", "Jun 2015", "Dec 2015", "Jun 2016", "Dec 2016", "Jun 2017"];
var attribute_names = ["Download", "Upload"]

var legend_grouping = { "MLab_DL": "legend_mbps", "MLab_UL": "legend_mbps", "FCC_DL": "legend_mbps", "FCC_UL": "legend_mbps", "broadband_cutoffs": "legend_mbps" }; //need to add in others

var legend_dict = { "MLab_DL": "DL", "MLab_UL": "UL", "FCC_DL": "DL", "FCC_UL": "UL", "Diff_DL": "Diff_DL", "Diff_UL": "Diff_UL", "broadband_cutoffs": "broadband_cutoffs" };
var standalone_admissible_toggles = { "broadband_cutoffs": ["county", "state_senate", "state_house"] } //this is a nice way to enable and disable toggles for standalones

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

var center_array:[[string, LngLat]] = [
    ["Alabama", new LngLat(32.806671, -86.791130)],
    ["Alaska", new LngLat(61.370716, -152.404419)],
    ["Arizona", new LngLat(33.729759, -111.431221)],
    ["Arkansas", new LngLat(34.969704, -92.373123)],
    ["California", new LngLat(36.116203, -119.681564)],
    ["Colorado", new LngLat(39.059811, -105.311104)],
    ["Connecticut", new LngLat(41.597782, -72.755371)],
    ["Delaware", new LngLat(39.318523, -75.507141)],
    ["District of Columbia", new LngLat(38.897438, -77.026817)],
    ["Florida", new LngLat(27.766279, -81.686783)],
    ["Georgia", new LngLat(33.040619, -83.643074)],
    ["Hawaii", new LngLat(21.094318, -157.498337)],
    ["Idaho", new LngLat(44.240459, -114.478828)],
    ["Illinois", new LngLat(40.349457, -88.986137)],
    ["Indiana", new LngLat(39.849426, -86.258278)],
    ["Iowa", new LngLat(42.011539, -93.210526)],
    ["Kansas", new LngLat(38.526600, -96.726486)],
    ["Kentucky", new LngLat(37.668140, -84.670067)],
    ["Louisiana", new LngLat(31.169546, -91.867805)],
    ["Maine", new LngLat(44.693947, -69.381927)],
    ["Maryland", new LngLat(39.063946, -76.802101)],
    ["Massachusetts", new LngLat(42.230171, -71.530106)],
    ["Michigan", new LngLat(43.326618, -84.536095)],
    ["Minnesota", new LngLat(45.694454, -93.900192)],
    ["Mississippi", new LngLat(32.741646, -89.678696)],
    ["Missouri", new LngLat(38.456085, -92.288368)],
    ["Montana", new LngLat(46.921925, -110.454353)],
    ["Nebraska", new LngLat(41.125370, -98.268082)],
    ["Nevada", new LngLat(38.313515, -117.055374)],
    ["New Hampshire", new LngLat(43.452492, -71.563896)],
    ["New Jersey", new LngLat(40.298904, -74.521011)],
    ["New Mexico", new LngLat(34.840515, -106.248482)],
    ["New York", new LngLat(42.165726, -74.948051)],
    ["North Carolina", new LngLat(35.630066, -79.806419)],
    ["North Dakota", new LngLat(47.528912, -99.784012)],
    ["Ohio", new LngLat(40.388783, -82.764915)],
    ["Oklahoma", new LngLat(35.565342, -96.928917)],
    ["Oregon", new LngLat(44.572021, -122.070938)],
    ["Pennsylvania", new LngLat(40.590752, -77.209755)],
    ["Rhode Island", new LngLat(41.680893, -71.511780)],
    ["South Carolina", new LngLat(33.856892, -80.945007)],
    ["South Dakota", new LngLat(44.299782, -99.438828)],
    ["Tennessee", new LngLat(35.747845, -86.692345)],
    ["Texas", new LngLat(31.054487, -97.563461)],
    ["Utah", new LngLat(40.150032, -111.862434)],
    ["Vermont", new LngLat(44.045876, -72.710686)],
    ["Virginia", new LngLat(37.769337, -78.169968)],
    ["Washington", new LngLat(47.400902, -121.490494)],
    ["West Virginia", new LngLat(38.491226, -80.954453)],
    ["Wisconsin", new LngLat(44.268543, -89.616508)],
    ["Wyoming", new LngLat(42.755966, -107.302490)]
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
            center: [
                fly_to_loc[1],
                fly_to_loc[0]
            ],
            zoom: 6,
            speed: .5
        });
    }

});

var colorArrayTime = [];
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
var map_div = $(".map-overlay_1");
createSlider(map_div, active_date);
makeToggle(source_ids, source_names, active_source, 'source_menu', true)
makeToggle(geo_ids, geo_names, active_geo, "geo_menu", false)
makeToggle(attribute_ids, attribute_names, active_attribute, "attribute_menu", true)
createLegend(eval(legend_dict[active_source + "_" + active_attribute] + "_" + "labels"), eval(legend_dict[active_source + "_" + active_attribute] + "_" + "colors"), starting_map);

interface County {
    county: string[];
    speed_mlab: number[];
    speed_mlab_up: number[];
    speed_477: number[];
    speed_477_up: number[];
    speed_diff: number[];
    speed_diff_up: number[];
    speed_diff_perc: number[];
    speed_diff_perc_up: number[];
    counts: number[];
    date_range: string[];
    nine_speed: number[];
    nine_up_speed: number[];
    broadband_cutoffs: string[];
}

export interface House {
    house_num: string[];
    house: string[];
    speed_mlab_up: number[];
    speed_477: number[];
    speed_477_up: number[];
    speed_diff: number[];
    speed_diff_up: number[];
    speed_diff_perc: number[];
    speed_diff_perc_up: number[];
    broadband_cutoffs: string[];
    date_range: string[];
    speed_mlab: number[];
    counts: number[];
}

interface downloadData {
    county: County[];
    house_: House[];
}

interface globalData {
    county: County[];
    state_house: House[];
    state_senate: House[];
}

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

        var workerSends: number[] = [];
        // Possible future enhancement: move the filtering in the below three lines into Workers as well
        for (var j = 0; j < date_ids.length; j++) {
            var filtered_house = global_data["state_house"].filter(function(entry) { return entry["date_range"] == [date_ids[j]]; });
            var filtered_senate = global_data["state_senate"].filter(function(entry) { return entry["date_range"] == [date_ids[j]]; });
            var filtered_county = global_data["county"].filter(function(entry) { return entry["date_range"] == [date_ids[j]]; });
            var data_filtered = { "county": filtered_county, "state_house": filtered_house, "state_senate": filtered_senate }
            var colorArray = [];
            workerSends[j] = 0;
            for (var i = 0; i < geo_ids.length; i++) {
                var colorWorker = new Worker("worker.js");
                colorWorker.onmessage = function (e) {
                    colorArray[e.data.geo_id] = e.data.speed_data;
                    workerSends[e.data.date_id]--;
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
                    date_id: j,
                    geo_id: i,
                });
                workerSends[j]++;
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
                    "visibility": "none",
                    "paint": {
                        "fill-color": "#d07386",
                        "fill-outline-color": "#cecece"
                    }
                }, "border-admin-3-4-case");

                makeStandalone("muni_networks", "Municipal Broadband", "muni_menu", false, false)

                turnOffOtherMaps(date_ids[0] + "_" + geo_ids[0] + "_" + source_ids[0] + "_" + attribute_ids[0]);

                //add story popups
                //make story popups work
                map.on('click', 'places', function(e) {
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
                        .setLngLat(marker.geometry.coordinates)
                        .setPopup(new mapboxgl.Popup({ offset: 25 }) // add popups
                            .setHTML('<h3>' + marker.properties.title + '</h3><p>' + marker.properties.description + '</p>'))
                        .addTo(map);
                });
            } // hasnt run
        } // is loaded 
    } // loaded
}); //render
