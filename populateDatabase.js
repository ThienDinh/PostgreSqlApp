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
		'app_name varchar(25) not null,'+
		'run_name varchar(25) not null,'+
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
		'app_name varchar(25) not null,'+
		'run_name varchar(25) not null,'+
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

// Load the requested table from data log file into the database.
function loadTable(table_name) {
	// Open file to read.
	const rl = readline.createInterface({
		input: fs.createReadStream('./datalogfiles/' + table_name + '.txt'),
		output: process.stdout,
		terminal: false
		// input: fs.createReadStream('./datalogfiles/fake.txt')
	});
	var fd = fs.openSync('./datalogfiles/' + table_name + '_modified.txt', 'w');
	var counter = 0;
	rl.on('line', (line) => {
				counter = counter + 1;
				// If it is the first line in the data log, we skip it.
				if (counter === 1) return;

				line = line.replace(/\t\t\$/g, '\t$');
				line = line.replace(/\t\t\//g, '\t/');				
				line = line.replace(/\t/g,'"');
				// Split the array using the character Tab '\t' as the delimiter.
				var arr = line.split('"');
				var edited_line = joinArray(arr, '"');
				// console.log(edited_line);
				// Balance the number of separators.
				/*
					Noted that there are 3 values that can be converted into integer.
					From the two 'y/n', we can go backward and forward.
					Also, after the path, must be the command, which is only one command, and the rest is parameters.
				*/
				// arr = balance_parameters(arr);

				var pattern1 = /\"\$\w+\"\"/;
				var pattern2 = /\"\/\w+\"\"/;
				if(pattern1.test(line)) {
					// console.log(line);
					var extract = pattern1.exec(line);
					line = line.replace(pattern1, '"$' + /\w+/i.exec(extract) +'"');
					// console.log('Edited');
					// console.log(new_line);
				} else if (pattern2.test(line)) {
					var extract = pattern2.exec(line);
					line = line.replace(pattern2, '"/' + /\w+/i.exec(extract) +'"');					
				}
				line = line.replace(/\"/g, '\t');

				fs.write(fd, line+'\n');
	});
//1519120014
//1524720016
// 16600001
	rl.on('close', () => {
		console.log('End reading lines from text file.');
		fs.closeSync(fd);
		console.log('Calling PostgreSQL COPY function.');
		// client.query("", function(err, result) {
			// console.log(result);
		// });
		// Close database connection.
		// client.end();
		console.log('Program finished!');
	});

}

function joinArray(arr, delimiter) {
	var join = arr[0];
	for(var i = 1; i < arr.length; i++) {
		join = join.concat(delimiter + arr[i]);
	}
	return join;
}

// Remove empty string parameters.
function balance_parameters(arr) {
	var total = arr.length;
	// var begin = 0;
	// var mid;
	// var end = ;

	for(var i = 0; i < arr.length; i++) {

	}
	return splited_line;
}

// Convert a string into an array using the provided delimiter.
function toArray(line, delimiter) {
	var arr = line.split(delimiter);
	return arr;
}

// Formuate the INSERT query to insert data into our database.
function formulateInsertMacro(table, arr_values) {
	return ('insert into ' + table + ' values (' + arr_values + ');');
}

// Load all tables.
function loadTables() {
	// Fail because 'Driver Step ID' column is not in table schema from the log file.
	loadTable('c_driver_step');

	// Fail because of the header.
	// loadTable('c_driver_step_detail');

	// Fail because of the header.
	// loadTable('c_driver_step_detail_h');

	// Successful without the header.
	// loadTable('c_app_run_dependency');

	// Fail because the database schema lacks of the last two columns in the datalog.
	// loadTable('c_driver_schedule');
}

// Execute functions.
dropTables();
createTables();
loadTables();

// copy c_app_run_dependency from 'C:\Users\tdinh\Documents\Code\NodeJS\PostgreSqlApp\DataLogFiles\c_app_run_dependency_modified.txt' (NULL '?');
// copy c_driver_step from 'C:\Users\tdinh\Documents\Code\NodeJS\PostgreSqlApp\DataLogFiles\c_driver_step_modified.txt' (NULL '?');
// copy c_driver_step_detail from 'C:\Users\tdinh\Documents\Code\NodeJS\PostgreSqlApp\DataLogFiles\c_driver_step_detail_modified.txt' (NULL '?');
// copy c_driver_step_detail_h from 'C:\Users\tdinh\Documents\Code\NodeJS\PostgreSqlApp\DataLogFiles\c_driver_step_detail_h_modified.txt' (NULL '?');
// copy c_driver_schedule from 'C:\Users\tdinh\Documents\Code\NodeJS\PostgreSqlApp\DataLogFiles\c_driver_schedule_modified.txt' (NULL '?');