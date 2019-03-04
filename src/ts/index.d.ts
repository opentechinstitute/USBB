export as namespace USB;

export interface County {
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

export interface downloadData {
    county: County[];
    house_: House[];
}

export interface globalData {
    county: County[];
    state_house: House[];
    state_senate: House[];
}

export interface filteredData {
    county: County[];
    state_house: House[];
    state_senate: House[];
    [key: string]: County[] | House[];
}