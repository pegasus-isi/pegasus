"use strict";

function timeChartToggle (choice)
{
	var chart = timeChart;
	var opt = timeChartOpt;
	
	if (choice == 1)
	{
		chart.setTitle ({ text: 'Time Chart by Jobs'});
		var count   = chart.series [1].data;
		var runtime = chart.series [0].data;

		for (var i  = 0; i < count.length; ++i)
		{
			count [i].update (timeChartData [i].count.job);
			runtime [i].update (timeChartData [i].total_runtime.job);
		}
	}
	else
	{
		chart.setTitle ({ text: 'Time Chart by Invocations' });
		var count   = chart.series [1].data;
		var runtime = chart.series [0].data;

		for (var i  = 0; i < count.length; ++i)
		{
			count [i].update (timeChartData [i].count.invocation);
			runtime [i].update (timeChartData [i].total_runtime.invocation);
		}
	}
}

function getTimeChartCategories ()
{
	var categories = [];
	for (var i = 0; i < timeChartData.length; ++i)
	{
		categories.push (timeChartData [i].date_format);
	}
	
	return categories;
}

function getTimeChartJobCount ()
{
	var data = [];
	
	for (var i = 0; i < timeChartData.length; ++i)
	{
		data.push (timeChartData [i].count.job);
	}
	
	return data;
}

function getTimeChartJobRuntime ()
{
	var data = [];
	
	for (var i = 0; i < timeChartData.length; ++i)
	{
		data.push (timeChartData [i].total_runtime.job);
	}
	
	return data;
}

function plotTimeChart ()
{
	getTimeChartData ();
	
	var categories = getTimeChartCategories ();
	var init_job_count =  getTimeChartJobCount ();
	var init_job_time =  getTimeChartJobRuntime ();
	
	timeChartOpt =
	{
		chart:
		{
			renderTo: 'time_chart'
		},
		credits : 
		{
			enabled : false
		},
		title: 
		{
			text: 'Time Chart by Jobs'
		},
        tooltip: 
        {
			formatter: function() 
			{
                return this.x +': '+ this.y + (this.series.name == 'Count' ? '' : ' secs');
            }
        },
		xAxis: [
		{
			categories: categories
		}],
		yAxis: [
		{
			labels:
			{
				formatter: function ()
				{
					return this.value;
				},
				style:
				{
					color: '#4572A7'
				}
			},
			title:
			{
				text: 'Runtime (secs)',
				style:
				{
					color: '#4572A7'
				}
			}
		},
		{
			title:
			{
				text: 'Count',
				style:
				{
					color: '#9A1919'
				}
			},
			labels:
			{
				style:
				{
					color: '#9A1919'
				}
			},
			opposite: true
		}],
		series: [
				{
					name: 'Runtime',
					color: Highcharts.getOptions ().colors [0],
					type: 'column',
					data: init_job_time
				},
				{
					name: 'Count',
					color: Highcharts.getOptions ().colors [1],
					type: 'column',
					yAxis: 1,
					data: init_job_count
				}]
	};
	
	timeChart = new Highcharts.Chart (timeChartOpt);
}

function getGanttChartCategories ()
{
	var categories = [];
	
	for (var i = 0; i < ganttChartData.length; ++i)
	{
		categories.push (ganttChartData [i].job_name);
	}
	
	return categories;
}

function getGanttSeries ()
{
	var series = [];

	var job = [];
	var pre = [];
	var condor = [];
	var grid = [];
	var exec = [];
	var kickstart = [];
	var post = [];
	var start = 0;
	
	for (var i = 0; i < ganttChartData.length; ++i)
	{
		if (i == 0)
		{
			start = ganttChartData [i].jobS;
		}
		job.push ([ganttChartData [i].jobS -start, ganttChartData [i].jobDuration]);
		pre.push ([ganttChartData [i].pre_start -start, ganttChartData [i].pre_duration]);
		condor.push ([ganttChartData [i].condor_start -start, ganttChartData [i].condor_duration]);
		grid.push ([ganttChartData [i].grid_start -start, ganttChartData [i].grid_duration]);
		exec.push ([ganttChartData [i].exec_start -start, ganttChartData [i].exec_duration]);
		kickstart.push ([ganttChartData [i].kickstart_start -start, ganttChartData [i].kickstart_duration]);
		post.push ([ganttChartData [i].post_start -start, ganttChartData [i].post_duration]);
	}
	
	series.push ({name:"Job", data: job});
	series.push ({name:"Condor", data: condor});
	//series.push ({name:"Grid", data: grid});
	//series.push ({name:"Pre", data: pre});
	//series.push ({name:"Exec", data: exec});
	//series.push ({name:"Kickstart", data: kickstart});
	//series.push ({name:"Post", data: post});
	
	return series;
}

function plotGanttChart ()
{
	getGanttChartData ();
	
	var categories = getGanttChartCategories ();
	var series = getGanttSeries ();
	
	ganttChartOpt =
	{
		chart:
		{
			renderTo: 'gantt_chart',
			type: 'columnrange',
            zoomType: 'y',
            inverted: true
		},
		credits : 
		{
			enabled : false
		},
		title: 
		{
			text: 'Workflow Execution Gantt Chart'
		},
        tooltip: 
        {
        	valueSuffix: ' seconds'
        },
		xAxis: 
		{
			categories: categories
		},
		yAxis: 
		{
			min: 0,
			//type: 'logarithmic',
            title: 
            {
                text: 'Timeline (Seconds)'
            }
        },
        plotOptions: 
        {
            columnrange: 
            {
            	//grouping: false, //Buggy
                dataLabels: 
                {
                    enabled: false,
                }
            }
        },
        legend: 
        {
            enabled: true
        },
        series: series
	};
	
	ganttChart = new Highcharts.Chart (ganttChartOpt);
}

function jobDistributionGraphToggle (choice)
{
	var chart = jobDistribution;
	var opt = jobDistributionOpt;
	
	if (choice == 1)
	{
		chart.setTitle ({ text: 'Job Distribution by Count'});
		var data = jobDistribution.series [0].data;

		for (var i  = 0; i < data.length; ++i)
		{
			data [i].update (jobDistributionData [i].count.total);
		}
	}
	else
	{
		chart.setTitle ({ text: 'Job Distribution by Time' });
		var data = jobDistribution.series [0].data;

		for (var i  = 0; i < data.length; ++i)
		{
			data [i].update (jobDistributionData [i].time.total);
		}
	}
}

function jobDistributionTooltipFormat ()
{
	var tip = '';
	tip += '<b>' + this.point.name + '</b><br/>';

	tip += 'Total Count: ' + this.point.count.total + '<br/>';
	tip += 'Succeded Count: ' + this.point.count.success + '<br/>';
	tip += 'Failed Count: ' + this.point.count.failure + '<br/>';

	tip += 'Min Runtime: ' + this.point.time.min + '<br/>';
	tip += 'Average Runtime: ' + this.point.time.avg + '<br/>';
	tip += 'Max Runtime: ' + this.point.time.max + '<br/>';
	tip += 'Total Runtime: ' + this.point.time.total + '<br/>';

	return tip;
}

function radializeChartColors ()
{
	// Radialize the colors
	Highcharts.getOptions ().colors = $.map (Highcharts.getOptions ().colors, function (color)
	{
		return {
			radialGradient:
			{
				cx: 0.5,
				cy: 0.3,
				r: 0.7
			},
			stops: 
				[
					[0, color],
					[1, Highcharts.Color (color).brighten (-0.3).get ('rgb')] // Darken
				]
		};
	});
}

function debug (obj)
{
	var s;
	for (i in obj)
	{
		s = i + ' ' + obj [i] + '\n';
	}

	alert (s);
}