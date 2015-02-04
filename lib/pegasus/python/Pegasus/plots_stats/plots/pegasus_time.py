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

logger = logging.getLogger(__name__)

from Pegasus.plots_stats import utils as plot_utils
from Pegasus.tools import utils

#Global variables----
prog_base = os.path.split(sys.argv[0])[1]	# Name of this program
output_dir = None


#----------print workflow details--------
def print_workflow_details(workflow_stat , output_dir):
	"""
	Prints the data required for generating the time chart into data file.
	@param workflow_stat the WorkflowInfo object reference 
	@param output_dir output directory path
	"""
	job_info_day =  "var data_job_per_day = [" + workflow_stat.get_formatted_job_instances_over_time_data('day') + "];\n"
	invocation_info_day =  "var data_invoc_per_day = [" + workflow_stat.get_formatted_invocations_over_time_data('day') + "];\n"
	job_info_hour =  "var data_job_per_hour = [" + workflow_stat.get_formatted_job_instances_over_time_data('hour') + "];\n"
	invocation_info_hour =  "var data_invoc_per_hour = [" + workflow_stat.get_formatted_invocations_over_time_data('hour') + "];\n"
	job_info_day_metadata =  "var job_metadata_per_day = [" + workflow_stat.get_formatted_job_instances_over_time_metadata('day') + "];\n"
	invocation_info_day_metadata =  "var invoc_metadata_per_day = [" + workflow_stat.get_formatted_invocations_over_time_metadata('day') + "];\n"
	job_info_hour_metadata =  "var job_metadata_per_hour = [" + workflow_stat.get_formatted_job_instances_over_time_metadata('hour') + "];\n"
	invocation_info_hour_metadata =  "var invoc_metadata_per_hour = [" + workflow_stat.get_formatted_invocations_over_time_metadata('hour') + "];\n"
	# print javascript file
	data_file = os.path.join(output_dir,  "tc_data.js")
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
	"""
	Generates the action script file which contains the javascript functions used by the main html file.
	@param output_dir output directory path
	"""
	action_content = """
	
function tc_fadeRight(){
	if(tc_curX == 0){
		return "images/right-fade.png"
	}
	return "images/right.png"
}

function tc_fadeDown(){
	if(tc_curRuntimeY == 0){
		return "images/down-fade.png"
	}
	return "images/down.png"
}

function tc_panLeft(){
	var tc_panBy = (tc_curEndX -tc_curX)/tc_panXFactor;
	tc_curX +=tc_panBy;
	tc_curEndX +=tc_panBy;
	tc_xScale.domain(tc_curX ,tc_curEndX );
	tc_rootPanel.render();
	tc_headerPanel.render();
}

function tc_panRight(){
	var tc_panBy = (tc_curEndX -tc_curX)/tc_panXFactor;
	if(tc_curX > 0){
		tc_curX -=tc_panBy;
		tc_curEndX -=tc_panBy;
		if(tc_curX <= 0){
		tc_curEndX += (tc_curX + tc_panBy)
		tc_curX = 0;
	}
	tc_xScale.domain(tc_curX ,tc_curEndX );
	tc_rootPanel.render();
	tc_headerPanel.render();
	}
}

function tc_panUp(){
	var tc_panRuntimeBy = (tc_curRuntimeEndY -tc_curRuntimeY)/tc_panYFactor;
	var tc_panCountBy = (tc_curCountEndY -tc_curCountY)/tc_panYFactor;
	tc_curRuntimeY +=tc_panRuntimeBy;
	tc_curRuntimeEndY += tc_panRuntimeBy;
	tc_curCountY+=tc_panCountBy;
	tc_curCountEndY += tc_panCountBy;
	tc_yRuntimeScale.domain(tc_curRuntimeY ,tc_curRuntimeEndY);
	tc_yCountScale.domain(tc_curCountY, tc_curCountEndY);
	tc_rootPanel.render();
	tc_headerPanel.render();
}

function tc_panDown(){
	var tc_panRuntimeBy = (tc_curRuntimeEndY -tc_curRuntimeY)/tc_panYFactor;
	var tc_panCountBy = (tc_curCountEndY -tc_curCountY)/tc_panYFactor;
	if(tc_curRuntimeY > 0){
		
		tc_curRuntimeY -= tc_panRuntimeBy;
		tc_curRuntimeEndY -= tc_panRuntimeBy;
		if(tc_curRuntimeY< 0){
			tc_curRuntimeEndY += (tc_curRuntimeY + panRuntimeBy);
			tc_curRuntimeY = 0;
		}
		tc_yRuntimeScale.domain(tc_curRuntimeY ,tc_curRuntimeEndY );
		
		tc_curCountY -= tc_panCountBy;
		tc_curCountEndY -= tc_panCountBy;
		if(tc_curCountY <0){
			tc_curCountEndY += (tc_curCountY+tc_panCountBy);
			tc_curCountY = 0;
		}
		tc_yCountScale.domain(tc_curCountY, tc_curCountEndY);
		tc_rootPanel.render();
		tc_headerPanel.render();
	}
}

function tc_zoomOut(){
	var tc_newX = 0;
	var tc_newRuntimeY = 0;
	var tc_newCountY = 0;
	
	tc_newX = tc_curEndX  + tc_curEndX*0.1;
	tc_newRuntimeY = tc_curRuntimeEndY  + tc_curRuntimeEndY*0.1;
	tc_newCountY = tc_curCountEndY+tc_curCountEndY*0.1; 
	if(tc_curX < tc_newX && isFinite(tc_newX)){
		tc_curEndX = tc_newX;
		tc_xScale.domain(tc_curX, tc_curEndX);
	}
	if(tc_curRuntimeY < tc_newRuntimeY && isFinite(tc_newRuntimeY)){
		tc_curRuntimeEndY = tc_newRuntimeY;
		tc_yRuntimeScale.domain(tc_curRuntimeY, tc_curRuntimeEndY);
	}
	if(tc_curCountY < tc_newCountY && isFinite(tc_newCountY)){
		tc_curCountEndY = tc_newCountY;
		tc_yCountScale.domain(tc_curCountY, tc_curCountEndY);
	}
	tc_rootPanel.render();
}

function tc_zoomIn(){
	var tc_newX = 0;
	var tc_newRuntimeY = 0;
	var tc_newCountY =0;
	tc_newX = tc_curEndX  - tc_curEndX*0.1;
	tc_newRuntimeY = tc_curRuntimeEndY  - tc_curRuntimeEndY*0.1;
	tc_newCountY = tc_curCountEndY - tc_curCountEndY*0.1; 
	if(tc_curX < tc_newX && isFinite(tc_newX)){
		tc_curEndX = tc_newX;
		tc_xScale.domain(tc_curX, tc_curEndX);
	}
	if(tc_curRuntimeY < tc_newRuntimeY && isFinite(tc_newRuntimeY)){
		tc_curRuntimeEndY =tc_newRuntimeY;
		tc_yRuntimeScale.domain(tc_curRuntimeY, tc_curRuntimeEndY);
	}
	if(tc_curCountY < tc_newCountY && isFinite(tc_newCountY)){
		tc_curCountEndY = tc_newCountY;
		tc_yCountScale.domain(tc_curCountY, tc_curCountEndY);
	}
	tc_rootPanel.render();
}

function tc_resetZooming(){
	tc_curX  = 0;
	tc_curEndX  = tc_dateTimeCount*tc_bar_spacing;
	tc_curRuntimeY = 0;
	tc_curRuntimeEndY =  tc_maxRuntime;
	tc_curCountY = 0;
	tc_curCountEndY =  tc_maxCount;
	tc_xScale.domain(tc_curX, tc_curEndX);
	tc_yCountScale.domain(tc_curCountY,tc_curCountEndY);
	tc_yRuntimeScale.domain(tc_curRuntimeY, tc_curRuntimeEndY);
	tc_rootPanel.render();
	tc_headerPanel.render();
}

function tc_setType(isJobSet){
	tc_isJob= isJobSet;
	tc_loadGraph();
}

function tc_setTime(isHourSet){
	tc_isHour = isHourSet;
	tc_loadGraph();
}

function tc_setChartTitle(){
	if(tc_isJob){
		if(tc_isHour){
			return "Job count/runtime grouped by hour";
		}else{
			return "Job count/runtime grouped by day";
		}
	}else{
		if(tc_isHour){
			return "Invocation count/runtime grouped by hour";
		}else{
			return "Invocation count/runtime grouped by day";
		}
	}
}

function tc_getMetaData(){
	if(tc_isJob){
		if(tc_isHour){
			return job_metadata_per_hour;
		}else{
			return job_metadata_per_day;
		}
	}else{
		if(tc_isHour){
			return invoc_metadata_per_hour;
		}else{
			return invoc_metadata_per_day;
		}
	}
		
}

function tc_getContentData(){
	if(tc_isJob){
		if(tc_isHour){
			return data_job_per_hour;
		}else{
			return data_job_per_day;
		}
	}else{
		if(tc_isHour){
			return data_invoc_per_hour;
		}else{
			return data_invoc_per_day;
		}
	}
}

function tc_loadGraph(){
	
	tc_metadata = tc_getMetaData();
	tc_dateTimeCount =tc_metadata[0].num;
	tc_maxCount = tc_metadata[0].max_count;
	tc_maxRuntime =tc_metadata[0].max_runtime;
	tc_maxRuntime +=  tc_maxRuntime/10;
	tc_maxCount += tc_maxCount/10;
	tc_resetZooming();
}

function tc_getData(){
	return tc_getContentData();
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
	"""
	Generates the header html content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	header_str = """
<html>
<head>
<title>"""+ workflow_stat.wf_uuid +"""</title>
<style type ='text/css'>
#time_chart{
border:1px solid orange;
}
#time_chart_footer_div{
border:1px solid #C35617;
}
</style>
</head>
<body>
<script type="text/javascript" src="js/protovis-r3.2.js"></script>
"""
	header_str += plot_utils.create_home_button()
	return header_str
	
def create_include(workflow_stat):
	"""
	Generates the html script include content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	include_str = """
<script type="text/javascript" src="tc_data.js"></script>
<script type="text/javascript" src="tc_action.js"></script>	
	"""
	return include_str
	
def create_variable(workflow_stat):
	"""
	Generates the javascript variables used to generate the chart.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	number_of_units = len(workflow_stat.wf_job_instances_over_time_statistics['hour'])
	max_count, max_runtime = workflow_stat.get_max_count_run_time(True, 'hour')
	# Adding  variables
	var_str = """
<script type='text/javascript'>
var tc_isJob = true;
var tc_isHour = true;
"""
	var_str += "\nvar tc_dateTimeCount =" + str(number_of_units) +";"
	var_str += "\nvar tc_maxRuntime =" + str( max_runtime)+";"
	var_str += "\nvar tc_maxCount = " + str( max_count)+";"
	var_str +="""
tc_maxRuntime +=  tc_maxRuntime/10;
tc_maxCount += tc_maxCount/10;
var tc_bar_spacing = 50;
var tc_single_bar_width = 20;
var tc_yScaleMargin  = 100;
var tc_xScaleBottomMargin = 120;
var tc_color =['steelblue','orange'];
var tc_desc=['Runtime in seconds','count'];
var tc_h = 840;
var tc_w = 1400;
var tc_toolbar_width = 550;
var tc_containerPanelPadding = 50;
var tc_chartPanelWidth = tc_w+ tc_containerPanelPadding*2;
var tc_chartPanelHeight  = tc_h + tc_containerPanelPadding*2;
var tc_curX  = 0;
var tc_curEndX  = tc_dateTimeCount*tc_bar_spacing;
var tc_curRuntimeY = 0;
var tc_curRuntimeEndY =  tc_maxRuntime;
var tc_curCountY = 0;
var tc_curCountEndY =  tc_maxCount;
var tc_xLabelPos = tc_containerPanelPadding + tc_yScaleMargin;
var tc_yLabelPos = 40;
var tc_labelWidth = 200;
var tc_panXFactor = 10;
var tc_panYFactor  = 10;
var tc_headerPanelWidth = tc_w+ tc_containerPanelPadding*2;
var tc_headerPanelHeight  = 100;
var tc_footerPanelWidth = tc_w+ tc_containerPanelPadding*2;
var tc_footerPanelHeight  = 60;
var tc_xScale = pv.Scale.linear(tc_curX, tc_curEndX).range(0, tc_w-2*tc_yScaleMargin);
var tc_yRuntimeScale = pv.Scale.linear(tc_curRuntimeY, tc_curRuntimeEndY).range(0, tc_h -tc_xScaleBottomMargin);
var tc_yCountScale = pv.Scale.linear(tc_curCountY, tc_curCountEndY).range(0, tc_h -tc_xScaleBottomMargin);
</script>
"""
	return var_str
	

def create_toolbar_panel(workflow_stat):
	"""
	Generates the top level toolbar content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	panel_str = """
<script type="text/javascript+protovis">
var tc_headerPanel = new pv.Panel()
	.width(tc_headerPanelWidth)
	.height(tc_headerPanelHeight)
	.fillStyle('white');

var tc_panPanel  = tc_headerPanel.add(pv.Panel)
	.left(tc_w + tc_containerPanelPadding -tc_toolbar_width)
	.width(tc_toolbar_width)
	.height(tc_headerPanelHeight);

tc_panPanel.add(pv.Image)
	.left(10)
	.top(34)
	.width(32)
	.height(32)
	.title('Pan left')
	.url('images/left.png').event('click', tc_panLeft);

tc_panPanel.add(pv.Image)
	.left(50)
	.top(34)
	.width(32)
	.height(32)
	.url(tc_fadeRight)
	.title('Pan right')
	.event('click', tc_panRight);

tc_panPanel.add(pv.Image)
	.left(90)
	.top(34)
	.width(32)
	.height(32)
	.url('images/up.png')
	.title('Pan up')
	.event('click', tc_panUp);
 
 tc_panPanel.add(pv.Image)
	.left(140)
	.top(34)
	.width(32)
	.height(32)
	.url(tc_fadeDown)
	.title('Pan down')
	.event('click', tc_panDown);

tc_panPanel.add(pv.Image)
	.left(190)
	.top(34)
	.width(32)
	.height(32)
	.url('images/zoom-in.png')
	.title('Zoom in')
	.event('click',tc_zoomIn);

tc_panPanel.add(pv.Image)
	.left(240)
	.top(34)
	.width(32)
	.height(32)
	.url('images/zoom-out.png')
	.title('Zoom out')
	.event('click', tc_zoomOut);

tc_panPanel.add(pv.Image)
	.left(290)
	.top(34)
	.width(32)
	.height(32)
	.url('images/zoom-reset.png')
	.title('Zoom reset')
	.event('click', tc_resetZooming);

tc_headerPanel.add(pv.Label)
	.top(40)
	.left( tc_containerPanelPadding + tc_yScaleMargin)
	.font(function() {return 24 +'px sans-serif';})
	.textAlign('left')
	.textBaseline('bottom')
	.text(function(){ return tc_setChartTitle();});

tc_headerPanel.add(pv.Label)
	.top(80)
	.left(tc_containerPanelPadding + tc_yScaleMargin)
	.font(function() {return 16 +'px sans-serif';})
	.textAlign('left')
	.textBaseline('bottom')
"""
	panel_str += ".text('" +workflow_stat.dax_label + "');\n"
	panel_str += """
tc_headerPanel.render();
</script>	
"""
	return panel_str

def create_chart_panel(workflow_stat):
	"""
	Generates the chart panel content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	panel_str ="""
<script type="text/javascript+protovis">
var tc_rootPanel = new pv.Panel()
	.width(tc_chartPanelWidth)
	.height(tc_chartPanelHeight)
	.fillStyle('white');

var tc_vis = tc_rootPanel.add(pv.Panel)
	.bottom(tc_containerPanelPadding)
	.top(tc_containerPanelPadding)
	.left(tc_containerPanelPadding)
	.width(tc_w)
	.height(tc_h)
	.fillStyle('white');

var tc_rulePanelH = tc_vis.add(pv.Panel)
	.overflow('hidden')
	.bottom(tc_xScaleBottomMargin);

tc_rulePanelH.add(pv.Rule)
	.left(tc_yScaleMargin - 20)
	.data(function()tc_yRuntimeScale.ticks())
	.strokeStyle(tc_desc[0])
	.width(20)
	.bottom(tc_yRuntimeScale)
	.anchor('left').add(pv.Label)
	.textBaseline('bottom')
	.text(tc_yRuntimeScale.tickFormat);

tc_rulePanelH.add(pv.Rule)
	.left( tc_w - tc_yScaleMargin)
	.data(function()tc_yCountScale.ticks())
	.strokeStyle(tc_desc[1])
	.width(20)
	.bottom(tc_yCountScale)
	.anchor('right').add(pv.Label)
	.textBaseline('bottom')
	.text(tc_yCountScale.tickFormat);

var tc_rulePanelV = tc_vis.add(pv.Panel)
	.overflow('hidden')
	.left(tc_yScaleMargin)
	.width(tc_w-2*tc_yScaleMargin)
	.bottom(0);

tc_rulePanelV.add(pv.Rule)
	.bottom(tc_xScaleBottomMargin)
	.data(function(){return tc_getData();})
	.strokeStyle('#F8F8F8')
	.left(function(){
	return tc_xScale(this.index*tc_bar_spacing) + tc_single_bar_width;
	})
	.height(tc_h )
	.anchor('bottom').add(pv.Label)
	.textAlign('left')
	.textBaseline("middle")
	.textAngle(Math.PI / 2)
	.text(function(d){return d.datetime;});

var tc_chartPanelContainer = tc_vis.add(pv.Panel)
	.left(tc_yScaleMargin)
	.bottom(tc_xScaleBottomMargin)
	.width(tc_w-2*tc_yScaleMargin)
	.strokeStyle('black')
	.overflow('hidden');

var tc_runtimePanel = tc_chartPanelContainer.add(pv.Panel)
	.data(function(){return tc_getData();});
  
tc_runtimePanel.add(pv.Bar)
	.bottom(0)
	.left(function(d){
	return tc_xScale(this.parent.index*tc_bar_spacing);
	})
	.width(tc_single_bar_width)
	.height(function(d){
	return tc_yRuntimeScale(d.runtime);})
	.fillStyle('steelblue')
	.anchor("top").add(pv.Label)
	.textAlign('middle')
	.textBaseline(function(d) "center")
	.textAngle(-Math.PI / 2)
	.text(function(d)d.runtime);
    
tc_runtimePanel.add(pv.Bar)
	.bottom(0)
	.left(function(d){
	return tc_xScale(this.parent.index*tc_bar_spacing) + tc_single_bar_width;
	})
	.width(tc_single_bar_width)
	.height(function(d){
	return tc_yCountScale(d.count);})
	.fillStyle('orange')
	.anchor("top").add(pv.Label)
	.textAlign('middle')
	.textBaseline(function(d) "center")
	.textAngle(-Math.PI / 2)
	.text(function(d)d.count);
    
 tc_rootPanel.add(pv.Label)
	.bottom(tc_containerPanelPadding + tc_xScaleBottomMargin)
	.font(function() {return 20 +'px sans-serif';})
	.textAlign('left')
	.textBaseline('top')
	.text('Runtime in seconds -->')
	.textAngle(-Math.PI / 2);

tc_rootPanel.add(pv.Label)
	.bottom(tc_containerPanelPadding + tc_xScaleBottomMargin)
	.left(tc_containerPanelPadding + tc_w)
	.font(function() {return 20 +'px sans-serif';})
	.textAlign('left')
	.textBaseline('top')
	.text('count -->')
	.textAngle(-Math.PI / 2);

tc_rootPanel.add(pv.Label)
	.left(tc_containerPanelPadding + tc_yScaleMargin)
	.bottom(0)
	.font(function() {return 20 +'px sans-serif';})
	.textAlign('left')
	.textBaseline('bottom')
	.text('Date time -->');
 
tc_rootPanel.render();
</script>
"""
	return panel_str
	

def create_legend_panel(workflow_stat):
	"""
	Generates the legend panel content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	panel_str ="""
<script type="text/javascript+protovis">
var tc_footerPanel = new pv.Panel()
	.width(tc_footerPanelWidth)
	.height(tc_footerPanelHeight)
	.fillStyle('white');
	tc_footerPanel.add(pv.Dot)
	.data(tc_desc)
	.left( function(d){
	if(this.index == 0){
		tc_xLabelPos = tc_containerPanelPadding +  tc_yScaleMargin;
		tc_yLabelPos = tc_footerPanelHeight-15;
	}else{
		if(tc_xLabelPos + tc_labelWidth > tc_w - (tc_containerPanelPadding + tc_yScaleMargin+ tc_labelWidth)){
			tc_xLabelPos =  tc_containerPanelPadding + tc_yScaleMargin;
			tc_yLabelPos -=15;
		}
		else{
			tc_xLabelPos += tc_labelWidth;
		}
	}
	return tc_xLabelPos;}
	)
	.bottom(function(d){
	return tc_yLabelPos;})
	.fillStyle(function(d) tc_color[this.index])
	.strokeStyle(null)
	.size(49)
	.anchor('right').add(pv.Label)
	.textMargin(6)
	.textAlign('left')
	.text(function(d) d);

tc_footerPanel.render();
</script>
	"""
	return panel_str


def create_bottom_toolbar():
	"""
	Generates the bottom toolbar html content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	toolbar_content ="""
<div id ='time_chart_footer_div' style='width: 1500px; margin : 0 auto;' >
	<div style ='float:right'>
		<div> Time filter </div>
		<input type='radio' name='time_filter' value='by day' onclick="tc_setTime(false);" /> by day<br />
		<input type='radio' name='time_filter' value='by hour' onclick="tc_setTime(true);" checked /> by hour<br />
	</div>
	<div>
		<div>Type filter</div>
		<input type='radio' name='type_filter' value='show jobs' onclick="tc_setType(true);" checked/> show jobs<br />
		<input type='radio' name='type_filter' value='show invocations' onclick="tc_setType(false);"/> show invocations<br />
	</div>
</div>
	
	""" 
	return toolbar_content





def create_time_plot(workflow_info , output_dir):
	"""
	Generates the html page content for displaying the time chart.
	@param workflow_stat the WorkflowInfo object reference 
	@output_dir the output directory path
	"""
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
	wf_content = "</div>\n"
	str_list.append(wf_content)
	wf_content =create_bottom_toolbar()
	str_list.append(wf_content)
	return "".join(str_list)
		
		
	

def create_time_plot_page(workflow_info ,output_dir):
	"""
	Prints the complete html page with the time chart and workflow details.
	@param workflow_stat the WorkflowInfo object reference 
	@output_dir the output directory path
	"""
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

def setup(submit_dir,out_dir, env):
	"""
	Setup the pegasus time module
	@param submit_dir submit directory path 
	@out_dir the output directory path
	@env the environment variables
	"""
	# global reference
	global output_dir
	output_dir = out_dir
	utils.create_directory(output_dir)
	src_js_path = env['pegasus_javascript_dir'] 
	src_img_path = os.path.join(env['pegasus_share_dir']  , "plots/images/protovis/")
	dest_js_path = os.path.join(output_dir, "js")
	dest_img_path = os.path.join(output_dir, "images/")
	utils.create_directory(dest_js_path)
	utils.create_directory(dest_img_path)
	plot_utils.copy_files(src_js_path , dest_js_path)
	plot_utils.copy_files(src_img_path, dest_img_path)
	# copy images from common
	src_img_path = os.path.join(env['pegasus_share_dir']  , "plots/images/common/")
	plot_utils.copy_files(src_img_path, dest_img_path) 
	create_action_script(output_dir)



def generate_chart(workflow_info):
	"""
	Generates the time chart and all it's required files
	@workflow_info WorkflowInfo object reference
	"""
	create_time_plot_page(workflow_info , output_dir)


# ---------main----------------------------------------------------------------------------
def main():
	sys.exit(0)


if __name__ == '__main__':
	main()
