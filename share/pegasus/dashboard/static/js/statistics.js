"use strict";

var workflowStats = { isLoaded : false };
var jobBreakdownStats = { isLoaded : false };
var jobStats = { isLoaded : false };
//var timeStats = { isLoaded : false };

function getWorkflowSummaryStats (container, url)
{
	var ajaxOpt = 
	{
		url : url,
		dataType: 'json',
		error: function (xhr, textStatus, errorThrown)
		{
			alert ('Error occured: ' + textStatus + ' ' + xhr.responseText);
		},
		success: function (data, textStatus, xhr) 
		{
			console.log (xhr.responseText);
			render_workflow_summary_stats (container, data);
		} 
	};
	
	$.ajax (ajaxOpt)
}

function render_workflow_summary_stats (container, data)
{
	var content = '';
	var dest = $('#' + container);
	
	if (data.length == 0)
	{
		content += 'No information available';
	}
	
	var content = '';
	content = '<table id="workflow_summary_stats_table" class="x">';
	content += '<tr>';
	content += '<th>Workflow Wall Time</th>';
	content += '<td>'+ formatData (data ['wall-time']) + '</td>';
	content += '</tr>';
	content += '<tr>';
	content += '<th>Workflow Cumulative Job Wall Time</th>';
	content += '<td>'+ formatData (data ['cum-time']) + '</td>';
	content += '</tr>';
	content += '<tr>';
	content += '<th>Cumulative Job Walltime as seen from Submit Side</th>';
	content += '<td>'+ formatData (data ['job-cum-time']) + '</td>';
	content += '</tr>';
	content += '<tr>';
	content += '<th>Workflow Retries</th>';
	content += '<td>'+ formatData (data ['retry-count']) + '</td>';
	content += '</tr>';
	content += '</table>';
	
	dest.html (content);
	
	verticalTableInit ('#workflow_summary_stats_table')
}

function getWorkflowStats (url)
{
	if (workflowStats.isLoaded)
	{
		return;
	}
	
	var ajaxOpt = 
	{
		url : url,
		dataType: 'json',
		error: function (xhr, textStatus, errorThrown)
		{
			alert ('Error occured: ' + textStatus + ' ' + xhr.responseText);
		},
		success: function (data, textStatus, xhr) 
		{
			render_workflow_stats (data);
			workflowStats.isLoaded = true;
		} 
	};
	
	$.ajax (ajaxOpt)
}

function render_workflow_stats (all_data)
{
	var content = '';
	var dest = $('#workflow_stats');
	
	var data = all_data.individual;
	
	if (data.length == 0)
	{
		content += 'No information available';
	}
	
	content += '<header class="title ui-widget-header">This Workflow</header>';
	content += render_workflow_stats_table ('individual', data) + '<br />';
	
	data = all_data.all;
	
	if (data.length == 0)
	{
		content += 'No information available';
	}
	
	content += '<header class="title ui-widget-header">Entire Workflow</header>';
	content += render_workflow_stats_table ('all', data);
	
	dest.html (content);
	
	$('#workflow_stats_individual_table').dataTable ({
		"bJQueryUI": true,
		"bSort": false,
		"bFilter": false,
		"bPaginate": false,
		"bInfo": false
	});
	
	$('#workflow_stats_all_table').dataTable ({
		"bJQueryUI": true,
		"bSort": false,
		"bFilter": false,
		"bPaginate": false,
		"bInfo": false
	});
}

function render_workflow_stats_table (type, data)
{
	var content = '';
	content = '<table id="workflow_stats_' + type + '_table">';
	content += '<thead><tr>';
	content += '<th>Type</th>';
	content += '<th>Succeeded</th>';
	content += '<th>Failed</th>';
	content += '<th>Incomplete</th>';
	content += '<th>Total</th>';
	content += '<th>Retries</th>';
	content += '<th>Total + Retries</th>';
	content += '</tr></thead>';
	content += '<tbody>';
	
	content += '<tr class="' + (data [0].total_failed_tasks === 0 ? 'successful' : 'failed') + '">';
	content += '<td>Tasks</td>';
	content += '<td>'+ formatData (data [0].total_succeeded_tasks) + '</td>';
	content += '<td>'+ formatData (data [0].total_failed_tasks) + '</td>';
	content += '<td>'+ formatData (data [0].total_unsubmitted_tasks) + '</td>';
	content += '<td>'+ formatData (data [0].total_tasks) + '</td>';
	content += '<td>'+ formatData (data [0].total_task_retries) + '</td>';
	content += '<td>'+ formatData (data [0].total_task_invocations) + '</td>';
	content += '</tr>';
	
	content += '<tr class="' + (data [1].total_failed_jobs === 0 ? 'successful' : 'failed') + '">';
	content += '<td>Jobs</td>';
	content += '<td>'+ formatData (data [1].total_succeeded_jobs) + '</td>';
	content += '<td>'+ formatData (data [1].total_failed_jobs) + '</td>';
	content += '<td>'+ formatData (data [1].total_unsubmitted_jobs) + '</td>';
	content += '<td>'+ formatData (data [1].total_jobs) + '</td>';
	content += '<td>'+ formatData (data [1].total_job_retries) + '</td>';
	content += '<td>'+ formatData (data [1].total_job_invocations) + '</td>';
	content += '</tr>';
	
	content += '<tr class="' + (data [2].total_failed_sub_wfs === 0 ? 'successful' : 'failed') + '">';
	content += '<td>Sub Workflows</td>';
	content += '<td>'+ formatData (data [2].total_succeeded_sub_wfs) + '</td>';
	content += '<td>'+ formatData (data [2].total_failed_sub_wfs) + '</td>';
	content += '<td>'+ formatData (data [2].total_unsubmitted_sub_wfs) + '</td>';
	content += '<td>'+ formatData (data [2].total_sub_wfs) + '</td>';
	content += '<td>'+ formatData (data [2].total_sub_wfs_retries) + '</td>';
	content += '<td>'+ formatData (data [2].total_sub_wfs_invocations) + '</td>';
	content += '</tr>';
	
	content += '</tbody></table>';
	
	return content;
}

function getJobBreakdownStats (url)
{
	if (jobBreakdownStats.isLoaded)
	{
		return;
	}
	
	var ajaxOpt = 
	{
		url : url,
		dataType: 'json',
		error: function (xhr, textStatus, errorThrown)
		{
			alert ('Error occured: ' + textStatus + xhr.responseText);
		},
		success: function (data, textStatus, xhr) 
		{
			render_job_breakdown (data);
			jobBreakdownStats.isLoaded = true;
		} 
	};
	
	$.ajax (ajaxOpt)
}

function render_job_breakdown (data)
{
	var dest = $('#job_breakdown_stats');
	
	if (data.length == 0)
	{
		dest.html ('No information available');
	}
	
	var content = '';
	content = '<table id="job_breakdown_stats_table">';
	content += '<thead><tr>';
	content += '<th>Transformation</th>';
	content += '<th>Count</th>';
	content += '<th>Succeeded</th>';
	content += '<th>Failed</th>';
	content += '<th>Min</th>';
	content += '<th>Max</th>';
	content += '<th>Mean</th>';
	content += '<th>Total</th>';
	content += '</tr></thead>';
	content += '<tbody>';
	
	for (var i = 0; i < data.length; ++i)
	{
		content += '<tr class="' + (data[i][3] === 0 ? 'successful' : 'failed') + '">';
		for (var j = 0; j < data [i].length; ++j)
		{
			content += '<td>';
			content += formatData (data [i][j]);
			content += '</td>';
		}
		content += '</tr>';
	}
	
	content += '</tbody></table>';
	dest.html (content);
	
	$('#job_breakdown_stats_table').dataTable ({
		"bJQueryUI": true,
		"sPaginationType": "full_numbers",
		"bProcessing": true,
		"bServerSide": false,
		"bAutoWidth": false
	});
}

function getJobStats (url)
{
	if (jobStats.isLoaded)
	{
		return;
	}
	
	var ajaxOpt = 
	{
		url : url,
		dataType: 'json',
		error: function (xhr, textStatus, errorThrown)
		{
			alert ('Error occured: ' + textStatus + ' ' + xhr.responseText);
		},
		success: function (data, textStatus, xhr) 
		{
			render_job_stats (data);
			jobStats.isLoaded = true;
		}
	};
	
	$.ajax (ajaxOpt)
}

function render_job_stats (data)
{
	var dest = $('#job_stats');
	
	if (data.length == 0)
	{
		dest.html ('No information available');
	}
	
	var content = '';
	content += '<table id="job_stats_table">';
	content += '<thead><tr>';
	content += '<th>Job</th>';
	content += '<th>Try</th>';
	content += '<th>Site</th>';
	content += '<th>Kickstart</th>';
	content += '<th>Multiplier</th>';
	content += '<th>Kickstart Multiplied</th>';
	content += '<th>CPU Time</th>';
	content += '<th>Post</th>';
	content += '<th>CondorQ Time</th>';
	content += '<th>Resource</th>';
	content += '<th>Runtime</th>';
	content += '<th>Seqexec</th>';
	content += '<th>Seqexec Delay</th>';
	content += '<th>Exitcode</th>';
	content += '<th>Host</th>';
	content += '</tr></thead>';
	content += '<tbody>';
	
	for (var i = 0; i < data.length; ++i)
	{
		content += '<tr class="' + (data[i][13] === 0 ? 'successful' : 'failed') + '">';
		for (var j = 0; j < data [i].length; ++j)
		{
			content += '<td>';
			content += formatData (data [i][j]);
			content += '</td>';
		}
		content += '</tr>';
	}
	
	content += '</tbody></table>';
	dest.html (content);
	
	$('#job_stats_table').dataTable ({
		"sScrollX": "100%",
        "bScrollCollapse": true,
		"bJQueryUI": true,
		"sPaginationType": "full_numbers",
		"bProcessing": true,
		"bServerSide": false
	});
}

/*
function getTimeStats ()
{
	if (timeStats.isLoaded)
	{
		return;
	}
	
	var ajaxOpt = 
	{
		url : '',
		dataType: 'json',
		error: function (xhr, textStatus, errorThrown)
		{
			alert ('Error occured: ' + textStatus + xhr.responseText);
		},
		success: function (data, textStatus, xhr) 
		{
		}
	};
	
	$.ajax (ajaxOpt)
}
*/

function activateEventHandler (event, ui)
{
	if (ui.newHeader.attr('title') == 'workflow_stats')
	{
		getWorkflowStats (ui.newHeader.attr ('href'));
	}
	else if (ui.newHeader.attr('title') == 'job_breakdown_stats')
	{
		getJobBreakdownStats (ui.newHeader.attr ('href'));
	}
	else if (ui.newHeader.attr('title') == 'job_stats')
	{
		getJobStats (ui.newHeader.attr ('href'));
	}
	else if (ui.newHeader.attr('title') == 'time_stats')
	{
		getTimeStats (ui.newHeader.attr ('href'));
	}
	else
	{
		alert ('Invalid accordian option ' + ui.newHeader.attr('title'));
	}
}

function formatData (num)
{
	if (typeof num === 'number' && num % 1 !== 0)
	{
		return num.toFixed (3);
	}
	
	return num;
}

function debug (obj)
{
	var i, s='';
	for (i in obj)
	{
		s += i + ' ' + obj [i] + '\n';
	}

	alert (s);
}