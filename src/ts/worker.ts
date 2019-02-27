this.onmessage = function(e) {
    let colors = createColorvector(e.data.filtered, e.data.names);
    this.postMessage({
        speed_data: colors,
        date_id: e.data.date_id,
        geo_id: e.data.geo_id,
    });
};

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

