var pg = require('pg');
var fs = require('fs');
const readline = require('readline');

var config = {
	user: 'postgres',
	password: 'abc12345',
	database: 'postgres',
	host: 'localhost',
	port: 5432
};

var client = new pg.Client(config);

client.on('drain', client.end.bind(client)); //disconnect client when all queries are finished
client.connect();
//Drop all tables.
function dropTables() {
	client.query('drop table c_driver_schedule; drop table c_driver_step; drop table c_driver_step_detail; drop table c_app_run_dependency; drop table c_driver_step_detail_h;', function(err, result) {
		// console.log('Result of dropTables(): ' + result);
	});
}

// Create all necessary tables.
function createTables() {
	// Table C_DRIVER_SCHEDULE.
	client.query("create table c_driver_schedule (" + 
		"audt_id decimal(18,0),"+
		"app_nme varchar(25),"+
		"run_nme varchar(25),"+
		"run_nbr integer,"+
		"re_run_nbr integer,"+
		"schdl_start_dtm time,"+
		"stts_cd varchar(40),"+
		"vlutn_start_dtm time,"+
		"vlutn_end_dtm time,"+
		"run_start_dtm time,"+
		"run_end_dtm time,"+
		"crt_dtm time,"+
		"lst_mdfd_dtm time,"+
		"app_run_id decimal(18,0),"+
		"sla_date date,"+
		"sla_time time);", function(erro, result) {
		// console.log(result);
	});
	// Table C_DRIVER_STEP.
	client.query('create table c_driver_step ('+
		'drvr_step_id decimal(18, 0),'+
		'app_nme varchar(25) not null,'+
		'run_nme varchar(25) not null,'+
		'grp_nbr integer not null,'+
		'grp_nme varchar(80),'+
		'run_order_nbr smallint not null,'+
		'path_txt varchar(80) null,'+
		'cmd_txt varchar(80) null,'+
		'prmtr_txt varchar(256),'+
		'grp_cncrrncy_ind varchar(2),'+
		'step_cncrrncy_ind varchar(2),'+
		'notify_txt varchar(128),'+
		'step_typ_cd varchar(40),'+
		'step_nme varchar(128),'+
		'err_prcs_nbr smallint,'+
		'crt_dtm timestamp,'+
		'lst_mdfd_dtm timestamp,'+
		'app_run_id decimal(18,0),'+
		'actv_step_ind varchar(2));', function(err, result) {
		// console.log(result);
	});
	// Table C_DRIVER_STEP_DETAIL.
	client.query('create table c_driver_step_detail ('+
		'drvr_step_dtl_id decimal(18,0),'+
		'audt_id decimal(18,0),'+
		'drvr_step_id decimal(18,0),'+
		'app_nme varchar(25) not null,'+
		'run_nme varchar(25) not null,'+
		'grp_nbr smallint not null,'+
		'run_order_nbr smallint not null,'+
		'run_stts_cd varchar(2) null,'+
		'err_prcs_nbr smallint,'+
		'sess_start_dtm timestamp,'+
		'sess_end_dtm timestamp,'+
		'run_start_dtm timestamp,'+
		'run_end_dtm timestamp,'+
		'crt_dtm timestamp,'+
		'lst_mdfd_dtm timestamp);', function (err, result) {
		// console.log(result);
	});
	// Table C_APP_RUN_DEPENDENCY.
	client.query('create table c_app_run_dependency ('+
		'run_app_dpndnc_id decimal(18,0),'+
		'app_nme varchar(25) not null,'+
		'run_nme varchar(25) not null,'+
		'dependant_app_nme varchar(25) not null,'+
		'dependant_run_nme varchar(25) not null,'+
		'crt_dtm timestamp,'+
		'lst_mdfd_dtm timestamp,'+
		'app_run_id decimal(18,0),'+
		'dependant_app_run_id decimal(18,0));', function(err, result){
		// console.log(result);
	});
	// Table C_DRIVER_STEP_DETAIL_H.
	client.query('create table c_driver_step_detail_h ('+
		'drvr_step_dtl_id decimal(18,0),'+
		'audt_id decimal(18,0),'+
		'drvr_step_id decimal(18,0),'+
		'app_nme varchar(25) not null,'+
		'run_nme varchar(25) not null,'+
		'grp_nbr integer not null,'+
		'run_order_nbr smallint not null,'+
		'run_stts_cd varchar(2) null,'+
		'err_prcs_nbr smallint,'+
		'sess_start_dtm timestamp,'+
		'sess_end_dtm timestamp,'+
		'run_start_dtm time,'+
		'run_end_dtm time,'+
		'crt_dtm time,'+
		'lst_mdfd_dtm time,'+
		'hist_dtm time);', function(err, result) {
		// console.log(result);
	});
}

var number_of_unsual_rows;
// Load the requested table from data log file into the database.
function loadTable(table_name) {
	// Open file to read.
	const rl = readline.createInterface({
		input: fs.createReadStream('./datalogfiles/' + table_name + '.txt'),
		output: process.stdout,
		terminal: false
		// input: fs.createReadStream('./datalogfiles/fake.txt')
	});
	var fd = fs.openSync('./datalogfiles/' + table_name + '_preprocessed.txt', 'w');
	var counter = 0;
	number_of_unsual_rows = 0;
	rl.on('line', (line) => {
				counter = counter + 1;
				// If it is the first line in the data log, we skip it.
				if (counter === 1) return;

				// Preprocess data in C_DRIVER_STEP.
				if(table_name === 'c_driver_step') {
					// Split the array using the character Tab '\t' as the delimiter.
					var arr = line.split('\t');
					// console.log(arr);
					var result = joinArray(arr, '~');
					var edited_line = result.result;
					if(result.isUnusual) {
						return;					
					}
					/*
						Noted that there are 3 values that can be converted into integer.
						From the two 'y/n', we can go backward and forward.
						Also, after the path, must be the command, which is only one command, and the rest is parameters.
					*/
				}
				fs.write(fd, line+'\n');

	});
//1519120014
//1524720016
// 16600001
	rl.on('close', () => {
		console.log('Prepocessing ' + table_name +' completed.');
		fs.closeSync(fd);
	});

}

// Join elements of an array into one line using the provided delimeter.
function joinArray(arr, delimiter) {
	var result = toNumberTypeArray(arr);
	var t_arr = result.result;
	var join = t_arr[0];
	for(var i = 1; i < arr.length; i++) {
		join = join + delimiter + t_arr[i];
	}
	// There are 19 columns in the table C_DRIVER_STEP.
	if(arr.length !== 19) {
		result.isUnusual = true;
	}
	// console.log(join);
	result.result = join;
	return result;
}

// Convert elements of the array to numbers.
function toNumberTypeArray(arr) {
	var t_arr = [];
	var counter = 0;
	var unusual = false;
	for(var i = 0; i < arr.length; i++) {
		var n = NaN;
		if(Number(arr[i]) === Number.parseInt(arr[i])) {
			counter = counter + 1;
			n = Number(arr[i]);
		}
		t_arr.push(n);
	}
	// Print lines that have unusual structure, which does not have exact 4 columns of integer types.
	if (counter !== 4) {
		console.log(t_arr.toString());
		unusual= true;
		// number_of_unsual_rows = number_of_unsual_rows + 1;
	}
	return {
		result: t_arr,
		isUnusual: unusual
	};
}

// Load all tables.
function loadTables() {

	// Still have trouble with it.
	loadTable('c_driver_step');

	// Loaded successfully.
	loadTable('c_driver_step_detail');

	// Loaded successfully.
	loadTable('c_driver_step_detail_h');

	// Loaded successfully.
	loadTable('c_app_run_dependency');

	// Loaded successfully.
	loadTable('c_driver_schedule');
}

// Execute functions.
dropTables();
createTables();
loadTables();

// copy c_app_run_dependency from 'C:\Users\tdinh\Documents\Code\NodeJS\PostgreSqlApp\DataLogFiles\c_app_run_dependency_preprocessed.txt' (NULL '?');
// copy c_driver_step from 'C:\Users\tdinh\Documents\Code\NodeJS\PostgreSqlApp\DataLogFiles\c_driver_step_preprocessed.txt' (NULL '?');
// copy c_driver_step_detail from 'C:\Users\tdinh\Documents\Code\NodeJS\PostgreSqlApp\DataLogFiles\c_driver_step_detail_preprocessed.txt' (NULL '?');
// copy c_driver_step_detail_h from 'C:\Users\tdinh\Documents\Code\NodeJS\PostgreSqlApp\DataLogFiles\c_driver_step_detail_h_preprocessed.txt' (NULL '?');
// copy c_driver_schedule from 'C:\Users\tdinh\Documents\Code\NodeJS\PostgreSqlApp\DataLogFiles\c_driver_schedule_preprocessed.txt' (NULL '?');