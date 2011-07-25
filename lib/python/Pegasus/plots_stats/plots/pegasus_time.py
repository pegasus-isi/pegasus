#!/usr/bin/env python
"""
Pegasus utility for generating workflow execution gantt chart

Usage: pegasus-gantt [options] submit directory

"""

##
#  Copyright 2010-2011 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

# Revision : $Revision$
import os
import sys
import logging
import optparse


# Initialize logging object
logger = logging.getLogger()
# Set default level to INFO
logger.setLevel(logging.INFO)

import common
from Pegasus.plots_stats import utils as plot_utils


#Global variables----
prog_base = os.path.split(sys.argv[0])[1]	# Name of this program
output_dir = None


def setup_logger(level_str):
	level_str = level_str.lower()
	if level_str == "debug":
		logger.setLevel(logging.DEBUG)
	if level_str == "warning":
		logger.setLevel(logging.WARNING)
	if level_str == "error":
		logger.setLevel(logging.ERROR)
	if level_str == "info":
		logger.setLevel(logging.INFO)
	return


#----------print workflow details--------
def print_workflow_details(workflow_stat , output_dir):
	job_info_day =  "var data_job_per_day = [" + workflow_stat.get_formatted_job_instances_over_time_data('day') + "];\n"
	invocation_info_day =  "var data_invoc_per_day = [" + workflow_stat.get_formatted_invocations_over_time_data('day') + "];\n"
	job_info_hour =  "var data_job_per_hour = [" + workflow_stat.get_formatted_job_instances_over_time_data('hour') + "];\n"
	invocation_info_hour =  "var data_invoc_per_hour = [" + workflow_stat.get_formatted_invocations_over_time_data('hour') + "];\n"
	job_info_day_metadata =  "var job_metadata_per_day = [" + workflow_stat.get_formatted_job_instances_over_time_metadata('day') + "];\n"
	invocation_info_day_metadata =  "var invoc_metadata_per_day = [" + workflow_stat.get_formatted_invocations_over_time_metadata('day') + "];\n"
	job_info_hour_metadata =  "var job_metadata_per_hour = [" + workflow_stat.get_formatted_job_instances_over_time_metadata('hour') + "];\n"
	invocation_info_hour_metadata =  "var invoc_metadata_per_hour = [" + workflow_stat.get_formatted_invocations_over_time_metadata('hour') + "];\n"
	# print javascript file
	data_file = os.path.join(output_dir,  "data.js")
	try:
		fh = open(data_file, "w")
		fh.write( "\n")
		fh.write(job_info_day)
		fh.write(invocation_info_day)
		fh.write(job_info_hour)
		fh.write(invocation_info_hour)
		fh.write(job_info_day_metadata)
		fh.write(invocation_info_day_metadata)
		fh.write(job_info_hour_metadata)
		fh.write(invocation_info_hour_metadata)
	except IOError:
		logger.error("Unable to write to file " + data_file)
		sys.exit(1)
	else:
		fh.close()	
	return
	


def create_action_script(output_dir):
	action_content = """
	
function fadeRight(){
	if(curX == 0){
		return "images/right-fade.png"
	}
	return "images/right.png"
}

function fadeDown(){
	if(curRuntimeY == 0){
		return "images/down-fade.png"
	}
	return "images/down.png"
}

function panLeft(){
	var panBy = (curEndX -curX)/panXFactor;
	curX +=panBy;
	curEndX +=panBy;
	xScale.domain(curX ,curEndX );
	rootPanel.render();
	headerPanel.render();
}

function panRight(){
	var panBy = (curEndX -curX)/panXFactor;
	if(curX > 0){
		curX -=panBy;
		curEndX -=panBy;
		if(curX <= 0){
		curEndX += (curX + panBy)
		curX = 0;
	}
	xScale.domain(curX ,curEndX );
	rootPanel.render();
	headerPanel.render();
	}
}

function panUp(){
	var panRuntimeBy = (curRuntimeEndY -curRuntimeY)/panYFactor;
	var panCountBy = (curCountEndY -curCountY)/panYFactor;
	curRuntimeY +=panRuntimeBy;
	curRuntimeEndY += panRuntimeBy;
	curCountY+=panCountBy;
	curCountEndY += panCountBy;
	yRuntimeScale.domain(curRuntimeY ,curRuntimeEndY);
	yCountScale.domain(curCountY, curCountEndY);
	rootPanel.render();
	headerPanel.render();
}

function panDown(){
	var panRuntimeBy = (curRuntimeEndY -curRuntimeY)/panYFactor;
	var panCountBy = (curCountEndY -curCountY)/panYFactor;
	if(curRuntimeY > 0){
		
		curRuntimeY -= panRuntimeBy;
		curRuntimeEndY -= panRuntimeBy;
		if(curRuntimeY< 0){
			curRuntimeEndY += (curRuntimeY + panRuntimeBy);
			curRuntimeY = 0;
		}
		yRuntimeScale.domain(curRuntimeY ,curRuntimeEndY );
		
		curCountY -= panCountBy;
		curCountEndY -= panCountBy;
		if(curCountY <0){
			curCountEndY += (curCountY+panCountBy);
			curCountY = 0;
		}
		yCountScale.domain(curCountY, curCountEndY);
		rootPanel.render();
		headerPanel.render();
	}
}

function zoomOut(){
	var newX = 0;
	var newRuntimeY = 0;
	var newCountY = 0;
	
	newX = curEndX  + curEndX*0.1;
	newRuntimeY = curRuntimeEndY  + curRuntimeEndY*0.1;
	newCountY = curCountEndY+curCountEndY*0.1; 
	if(curX < newX && isFinite(newX)){
		curEndX = newX;
		xScale.domain(curX, curEndX);
	}
	if(curRuntimeY < newRuntimeY && isFinite(newRuntimeY)){
		curRuntimeEndY = newRuntimeY;
		yRuntimeScale.domain(curRuntimeY, curRuntimeEndY);
	}
	if(curCountY < newCountY && isFinite(newCountY)){
		curCountEndY = newCountY;
		yCountScale.domain(curCountY, curCountEndY);
	}
	rootPanel.render();
}

function zoomIn(){
	var newX = 0;
	var newRuntimeY = 0;
	var newCountY =0;
	newX = curEndX  - curEndX*0.1;
	newRuntimeY = curRuntimeEndY  - curRuntimeEndY*0.1;
	newCountY = curCountEndY - curCountEndY*0.1; 
	if(curX < newX && isFinite(newX)){
		curEndX = newX;
		xScale.domain(curX, curEndX);
	}
	if(curRuntimeY < newRuntimeY && isFinite(newRuntimeY)){
		curRuntimeEndY = newRuntimeY;
		yRuntimeScale.domain(curRuntimeY, curRuntimeEndY);
	}
	if(curCountY < newCountY && isFinite(newCountY)){
		curCountEndY = newCountY;
		yCountScale.domain(curCountY, curCountEndY);
	}
	rootPanel.render();
}

function resetZooming(){
	curX  = 0;
	curEndX  = dateTimeCount*bar_spacing;
	curRuntimeY = 0;
	curRuntimeEndY =  maxRuntime;
	curCountY = 0;
	curCountEndY =  maxCount;
	xScale.domain(curX, curEndX);
	yCountScale.domain(curCountY,curCountEndY);
	yRuntimeScale.domain(curRuntimeY, curRuntimeEndY);
	rootPanel.render();
	headerPanel.render();
}

function setType(){
	if(isJob){
		isJob= false;
	}else{
		isJob= true;
	}
	loadGraph();
}

function setTime(){
	if(isHour){
		isHour = false;
	}else{
		isHour = true;
	}
	loadGraph();
}

function getMetaData(){
	if(isJob){
		if(isHour){
			return job_metadata_per_hour;
		}else{
			return job_metadata_per_day;
		}
	}else{
		if(isHour){
			return invoc_metadata_per_hour;
		}else{
			return invoc_metadata_per_day;
		}
	}
		
}

function getContentData(){
	if(isJob){
		if(isHour){
			return data_job_per_hour;
		}else{
			return data_job_per_day;
		}
	}else{
		if(isHour){
			return data_invoc_per_hour;
		}else{
			return data_invoc_per_day;
		}
	}
}

function loadGraph(){
	
	data = getContentData();
	metadata = getMetaData();
	dateTimeCount =metadata[0].num;
	maxCount = metadata[0].max_count;
	maxRuntime =metadata[0].max_runtime;
	resetZooming();
}

function getData(){
	return getContentData();
}

"""
	# print action script
	data_file = os.path.join(output_dir,  "tc_action.js")
	try:
		fh = open(data_file, "w")
		fh.write( action_content)
		fh.write( "\n")
	except IOError:
		logger.error("Unable to write to file " + data_file)
		sys.exit(1)
	else:
		fh.close()



def create_header(workflow_stat):
	header_str = """
<html>
<head>
<title>Time chart</title>
<style type ='text/css'>
#time_chart{
border:1px solid red;
}
</style>
</head>
<body>
<script type="text/javascript" src="js/protovis-r3.2.js"></script>
"""
	return header_str
	
def create_include(workflow_stat):
	include_str = """
<script type="text/javascript" src="data.js"></script>
<script type="text/javascript" src="tc_action.js"></script>	
	"""
	return include_str
	
def create_variable(workflow_stat):
	number_of_units = len(workflow_stat.wf_job_instances_over_time_statistics['hour'])
	max_count, max_runtime = workflow_stat.get_max_count_run_time(True, 'hour')
	# Adding  variables
	var_str = """
<script type='text/javascript'>
var isJob = true;
var isHour = true;
"""
	var_str += "\nvar dateTimeCount =" + str(number_of_units) +";"
	var_str += "\nvar maxRuntime =" + str( max_runtime)+";"
	var_str += "\nvar maxCount = " + str( max_count)+";"
	var_str +=""" 
var bar_spacing = 50;
var single_bar_width = 20;
var yScaleMargin  = 100;
var xScaleBottomMargin = 120;
var color =['steelblue','orange'];
var desc=['Runtime in seconds','transformation count'];
var h = 840;
var w = 1400;
var toolbar_width = 550;
var containerPanelPadding = 50;
var chartPanelWidth = w+ containerPanelPadding*2;
var chartPanelHeight  = h + containerPanelPadding*2;
var curX  = 0;
var curEndX  = dateTimeCount*bar_spacing;
var curRuntimeY = 0;
var curRuntimeEndY =  maxRuntime;
var curCountY = 0;
var curCountEndY =  maxCount;
var xLabelPos = containerPanelPadding + yScaleMargin;
var yLabelPos = 40;
var panXFactor = 10;
var panYFactor  = 10;
var headerPanelWidth = w+ containerPanelPadding*2;
var headerPanelHeight  = 100;
var footerPanelWidth = w+ containerPanelPadding*2;
var footerPanelHeight  = 60;
var xScale = pv.Scale.linear(curX, curEndX).range(0, w-2*yScaleMargin);
var yRuntimeScale = pv.Scale.linear(curRuntimeY, curRuntimeEndY).range(0, h -xScaleBottomMargin);
var yCountScale = pv.Scale.linear(curCountY, curCountEndY).range(0, h -xScaleBottomMargin);
</script>
"""
	return var_str
	

def create_toolbar_panel(workflow_stat):
	panel_str = """
<script type="text/javascript+protovis">
var headerPanel = new pv.Panel()
	.width(headerPanelWidth)
	.height(headerPanelHeight)
	.fillStyle('white');

var panPanel  = headerPanel.add(pv.Panel)
	.left(w + containerPanelPadding -toolbar_width)
	.width(toolbar_width)
	.height(headerPanelHeight);

panPanel.add(pv.Image)
	.left(10)
	.top(34)
	.width(32)
	.height(32)
	.title('Pan left')
	.url('images/left.png').event('click', panLeft);

panPanel.add(pv.Image)
	.left(50)
	.top(34)
	.width(32)
	.height(32)
	.url(fadeRight)
	.title('Pan right')
	.event('click', panRight);

panPanel.add(pv.Image)
	.left(90)
	.top(34)
	.width(32)
	.height(32)
	.url('images/up.png')
	.title('Pan up')
	.event('click', panUp);
 
 panPanel.add(pv.Image)
	.left(140)
	.top(34)
	.width(32)
	.height(32)
	.url(fadeDown)
	.title('Pan down')
	.event('click', panDown);

panPanel.add(pv.Image)
	.left(190)
	.top(34)
	.width(32)
	.height(32)
	.url('images/zoom-in.png')
	.title('Zoom in')
	.event('click', zoomIn);

panPanel.add(pv.Image)
	.left(240)
	.top(34)
	.width(32)
	.height(32)
	.url('images/zoom-out.png')
	.title('Zoom out')
	.event('click', zoomOut);

panPanel.add(pv.Image)
	.left(290)
	.top(34)
	.width(32)
	.height(32)
	.url('images/zoom-reset.png')
	.title('Zoom reset')
	.event('click', resetZooming);

headerPanel.add(pv.Label)
	.top(40)
	.left( containerPanelPadding + yScaleMargin)
	.font(function() {return 24 +'px sans-serif';})
	.textAlign('left')
	.textBaseline('bottom')
	.text('Time chart');

headerPanel.add(pv.Label)
	.top(80)
	.left(containerPanelPadding + yScaleMargin)
	.font(function() {return 16 +'px sans-serif';})
	.textAlign('left')
	.textBaseline('bottom')
"""
	panel_str += ".text('" +workflow_stat.dax_label + "');\n"
	panel_str += """
headerPanel.render();
</script>	
"""
	return panel_str

def create_chart_panel(workflow_stat):
	panel_str ="""
<script type="text/javascript+protovis">
var rootPanel = new pv.Panel()
	.width(chartPanelWidth)
	.height(chartPanelHeight)
	.fillStyle('white');

var vis = rootPanel.add(pv.Panel)
	.bottom(containerPanelPadding)
	.top(containerPanelPadding)
	.left(containerPanelPadding)
	.width(w)
	.height(h)
	.fillStyle('white');

var rulePanelH = vis.add(pv.Panel)
	.overflow('hidden')
	.bottom(xScaleBottomMargin);

rulePanelH.add(pv.Rule)
	.left(yScaleMargin - 20)
	.data(function()yRuntimeScale.ticks())
	.strokeStyle(desc[0])
	.width(20)
	.bottom(yRuntimeScale)
	.anchor('left').add(pv.Label)
	.textBaseline('bottom')
	.text(yRuntimeScale.tickFormat);

rulePanelH.add(pv.Rule)
	.left( w - yScaleMargin)
	.data(function()yCountScale.ticks())
	.strokeStyle(desc[1])
	.width(20)
	.bottom(yCountScale)
	.anchor('right').add(pv.Label)
	.textBaseline('bottom')
	.text(yCountScale.tickFormat);

var rulePanelV = vis.add(pv.Panel)
	.overflow('hidden')
	.left(yScaleMargin)
	.width(w-2*yScaleMargin)
	.bottom(0);

rulePanelV.add(pv.Rule)
	.bottom(xScaleBottomMargin)
	.data(function(){return getData();})
	.strokeStyle('#F8F8F8')
	.left(function(){
	return xScale(this.index*bar_spacing) + single_bar_width;
	})
	.height(h )
	.anchor('bottom').add(pv.Label)
	.textAlign('left')
	.textBaseline("middle")
	.textAngle(Math.PI / 2)
	.text(function(d){return d.datetime;});

var chartPanelContainer = vis.add(pv.Panel)
	.left(yScaleMargin)
	.bottom(xScaleBottomMargin)
	.width(w-2*yScaleMargin)
	.strokeStyle('black')
	.overflow('hidden');

var runtimePanel = chartPanelContainer.add(pv.Panel)
	.data(function(){return getData();});
  
runtimePanel.add(pv.Bar)
	.bottom(0)
	.left(function(d){
	return xScale(this.parent.index*bar_spacing);
	})
	.width(single_bar_width)
	.height(function(d){
	return yRuntimeScale(d.runtime);})
	.fillStyle('steelblue')
	.anchor("top").add(pv.Label)
	.textAlign('middle')
	.textBaseline(function(d) "center")
	.textAngle(-Math.PI / 2)
	.text(function(d)d.runtime);
    
runtimePanel.add(pv.Bar)
	.bottom(0)
	.left(function(d){
	return xScale(this.parent.index*bar_spacing) + single_bar_width;
	})
	.width(single_bar_width)
	.height(function(d){
	return yCountScale(d.count);})
	.fillStyle('orange')
	.anchor("top").add(pv.Label)
	.textAlign('middle')
	.textBaseline(function(d) "center")
	.textAngle(-Math.PI / 2)
	.text(function(d)d.count);
    
 rootPanel.add(pv.Label)
	.bottom(containerPanelPadding + xScaleBottomMargin)
	.font(function() {return 20 +'px sans-serif';})
	.textAlign('left')
	.textBaseline('top')
	.text('Runtime in seconds -->')
	.textAngle(-Math.PI / 2);

rootPanel.add(pv.Label)
	.bottom(containerPanelPadding + xScaleBottomMargin)
	.left(containerPanelPadding + w)
	.font(function() {return 20 +'px sans-serif';})
	.textAlign('left')
	.textBaseline('top')
	.text('count -->')
	.textAngle(-Math.PI / 2);

rootPanel.add(pv.Label)
	.left(containerPanelPadding + yScaleMargin)
	.bottom(0)
	.font(function() {return 20 +'px sans-serif';})
	.textAlign('left')
	.textBaseline('bottom')
	.text('Date time -->');
 
rootPanel.render();
</script>
"""
	return panel_str
	

def create_legend_panel(workflow_stat):
	panel_str ="""
<script type="text/javascript+protovis">
var footerPanel = new pv.Panel()
	.width(footerPanelWidth)
	.height(footerPanelHeight)
	.fillStyle('white');
	footerPanel.add(pv.Dot)
	.data(desc)
	.left( function(d){
	if(this.index == 0){
		xLabelPos = containerPanelPadding + yScaleMargin;
		yLabelPos = 30;
	}else{
		if(xLabelPos + 180 > w){
			xLabelPos =  containerPanelPadding + yScaleMargin;
			yLabelPos -=10;
		}
		else{
			xLabelPos += 180;
		}
	}
	return xLabelPos;}
	)
	.bottom(function(d){
	return yLabelPos;})
	.fillStyle(function(d) color[this.index])
	.strokeStyle(null)
	.size(30)
	.anchor('right').add(pv.Label)
	.textMargin(6)
	.textAlign('left')
	.text(function(d) d);

footerPanel.render();
</script>
	"""
	return panel_str


def create_bottom_toolbar():
	toolbar_content ="""
<div id ='tools' style='width: 300px; margin : 0 auto;' >
	<div style ='float:right'>
		<p1> Time filter </p1>
		<br/>
		<input type='radio' name='time_filter' value='by day' onclick="setTime();" /> by day<br />
		<input type='radio' name='time_filter' value='by hour' onclick="setTime();" checked /> by hour<br />
	</div>
	<div>
		<p1>Type filter</p1>
		<br/>
		<input type='radio' name='type_filter' value='show jobs' onclick="setType();" checked/> show jobs<br />
		<input type='radio' name='type_filter' value='show invocations' onclick="setType();"/> show invocations<br />
	</div>
</div>	
	""" 
	return toolbar_content





def create_time_plot(workflow_info , output_dir):
	print_workflow_details(workflow_info ,output_dir)
	str_list = []
	wf_content = create_include(workflow_info)
	str_list.append(wf_content)
	# Adding  variables
	wf_content =create_variable(workflow_info)
	str_list.append(wf_content)
	wf_content = "<div id ='time_chart' style='width: 1500px; margin : 0 auto;' >\n"
	str_list.append(wf_content)
	# adding the tool bar panel
	wf_content =create_toolbar_panel(workflow_info)
	str_list.append(wf_content)
	# Adding the chart panel
	wf_content =create_chart_panel(workflow_info)
	str_list.append(wf_content)
	# Adding the legend panel
	wf_content =create_legend_panel(workflow_info)
	str_list.append(wf_content)
	wf_content = "</div>\n<br />"
	str_list.append(wf_content)
	wf_content =create_bottom_toolbar()
	str_list.append(wf_content)
	return "".join(str_list)
		
		
	

def create_time_plot_page(workflow_info ,output_dir):
	
	str_list = []
	wf_page = create_header(workflow_info)
	str_list.append(wf_page)
	wf_page = create_time_plot(workflow_info ,output_dir)
	str_list.append(wf_page)
	# printing the brain dump content
	if workflow_info.submit_dir is None:
		logger.warning("Unable to display brain dump contents. Invalid submit directory for workflow  " + workflow_info.wf_uuid)
	else:
		wf_page = plot_utils.print_property_table(workflow_info.wf_env,False ," : ")
		str_list.append(wf_page)
	wf_page = "\n<div style='clear: left'>\n</div></body>\n</html>"
	str_list.append(wf_page)
	data_file = os.path.join(output_dir,  workflow_info.wf_uuid+".html")
	try:
		fh = open(data_file, "w")
		fh.write( "\n")
		fh.write("".join(str_list))	
	except IOError:
		logger.error("Unable to write to file " + data_file)
		sys.exit(1)
	else:
		fh.close()	
	return
def setup(submit_dir,out_dir,log_level):
	# global reference
	global output_dir
	output_dir = out_dir
	if log_level == None:
		log_level = "info"
	setup_logger(log_level)
	plot_utils.create_directory(output_dir)
	src_js_path = os.path.join(common.pegasus_home, "lib/javascript")
	src_img_path = os.path.join(common.pegasus_home, "share/plots/images/protovis/")
	dest_js_path = os.path.join(output_dir, "js")
	dest_img_path = os.path.join(output_dir, "images/")
	plot_utils.create_directory(dest_js_path)
	plot_utils.create_directory(dest_img_path)
	plot_utils.copy_files(src_js_path , dest_js_path)
	plot_utils.copy_files(src_img_path, dest_img_path) 	 	
	create_action_script(output_dir)



def generate_chart(workflow_info):
	create_time_plot_page(workflow_info , output_dir)


# ---------main----------------------------------------------------------------------------
def main():
	sys.exit(0)


if __name__ == '__main__':
	main()