"""
Pegasus utility for generating host over time chart

Usage: pegasus-gantt [options] submit directory

"""
from __future__ import absolute_import

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

logger = logging.getLogger(__name__)

from Pegasus.tools import utils
from Pegasus.plots_stats import utils as plot_utils
from . import populate
from datetime import timedelta
from datetime import datetime


#Global variables----
output_dir = None


#----------print workflow details--------
def print_workflow_details(workflow_stat ,output_dir , extn):
	"""
	Prints the data required for generating the host chart into data file.
	@param workflow_stat the WorkflowInfo object reference 
	@param output_dir output directory path
	"""
	job_info = "var hc_data = [" + workflow_stat.get_formatted_host_data(extn) + "];"
	# print javascript file
	data_file = os.path.join(output_dir,  "hc_"+workflow_stat.wf_uuid+"_data.js")
	try:
		fh = open(data_file, "w")
		fh.write( "\n")
		fh.write(job_info)
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
	# print action script
	action_content = "\n\
function hc_barvisibility(d , index){\n\
if(!d){\n\
 return false;\n\
}\n\
var yPos = index * hc_bar_spacing;\n\
if(yPos < hc_curY || yPos > hc_curEndY ){\n\
return false;\n\
}else{\n\
return true;\n\
}\n\
}\n\n\
function hc_openWF(url){\n\
if(hc_isNewWindow){\n\
window.open(url);\n\
}else{\n\
self.location = url;\n\
}\n\
}\n\n\
function hc_printJobDetails(d){\n\
var job_details = \"Job name :\"+d.name;\n\
if(d.gD !==''){\n\
job_details +=\"\\nResource delay :\"+d.gD +\" sec.\";\n\
}\n\
if(d.eD !==''){\n\
job_details +=\"\\nRuntime as seen by dagman :\"+d.eD +\" sec.\";\n\
}\n\
if(d.kD!==''){\n\
job_details +=\"\\nKickstart duration :\"+d.kD +\" sec.\";\n\
}\n\
job_details +=\"\\nMain task :\"+d.transformation ;\n\
alert(job_details);\n\
}\n\n\
function hc_getJobBorder(d){\n\
if(!d.state){\n\
return 'red';\n\
}\n\
else if(d.sub_wf){\n\
return 'orange';\n\
}\n\
if(d.transformation){\n\
return d.color;\n\
}else{\n\
return 'gray';\n\
}\n\
}\n\n\
function hc_getJobTime(d) {\n\
var jobWidth = 0;\n\
if(d.jobD){\n\
jobWidth = hc_xScale(d.jobS + d.jobD) -hc_xScale(d.jobS);\n\
}\n\
if(jobWidth > 0 && jobWidth < 1 ){\n\
	jobWidth = 1;\n\
}\n\
return jobWidth;\n\
}\n\n\
function hc_getCondorTime(d) {\n\
var cDWidth = 0;\n\
if(d.cD){\n\
cDWidth = hc_xScale(d.cS + d.cD) - hc_xScale(d.cS)\n\
}\n\
if(cDWidth > 0 && cDWidth < 1 ){\n\
cDWidth = 1;\n\
}\n\
return cDWidth;\n\
}\n\n\
function hc_getResourceDelay(d) {\n\
var gWidth = 0;\n\
if(d.gS){\n\
gWidth = hc_xScale(d.gS + d.gD) - hc_xScale(d.gS);\n\
}\n\
if(gWidth > 0 && gWidth < 1 ){\n\
	gWidth = 1;\n\
}\n\
return gWidth;\n\
}\n\n\
function hc_getRunTime(d) {\n\
var rtWidth = 0;\n\
if(d.eD){\n\
rtWidth = hc_xScale(d.eS + d.eD) -hc_xScale(d.eS);\n\
}\n\
if(rtWidth > 0 && rtWidth < 1 ){\n\
	rtWidth = 1;\n\
}\n\
return rtWidth;\n\
}\n\n\
function hc_getKickStartTime(d) {\n\
var kickWidth = 0;\n\
if(d.kD){\n\
kickWidth = hc_xScale(d.kS + d.kD) -hc_xScale(d.kS);\n\
}\n\
if(kickWidth > 0 && kickWidth < 1 ){\n\
	kickWidth = 1;\n\
}\n\
return kickWidth;\n\
}\n\n\
function hc_showState(){\n\
if(hc_condorTime || hc_kickstart || hc_condorRunTime || hc_resourceDelay){\n\
return true;\n\
}else{\n\
return false;\n\
}\n\
}\n\n\
function hc_setCondorTime(){\n\
if(hc_condorTime){\n\
hc_condorTime = false;\n\
}else{\n\
hc_condorTime = true;\n\
}\n\
hc_rootPanel.render();\n\
}\n\n\
function hc_setKickstart(){\n\
if(hc_kickstart){\n\
hc_kickstart = false;\n\
}else{\n\
hc_kickstart = true;\n\
}\n\
hc_rootPanel.render();\n\
}\n\n\
function hc_setCondorRuntime(){\n\
if(hc_condorRunTime){\n\
hc_condorRunTime = false;\n\
}else{\n\
hc_condorRunTime = true;\n\
}\n\
hc_rootPanel.render();\n\
}\n\n\
function hc_setResourceDelay(){\n\
if(hc_resourceDelay){\n\
hc_resourceDelay = false;\n\
}else{\n\
hc_resourceDelay = true;\n\
}\n\
hc_rootPanel.render();\n\
}\n\n\
function hc_setShowLabel(){\n\
if(hc_showName){\n\
	return 'Hide host name';\n\
}else{\n\
	return 'Show host name';\n\
}\n\
}\n\n\
function hc_setShowName(){\n\
if(hc_showName){\n\
	hc_showName = false;\n\
}else{\n\
	hc_showName = true;\n\
}\n\
hc_rootPanel.render();\n\
return;\n\
}\n\n\
function hc_fadeRight(){\n\
if(hc_curX == 0){\n\
	return \"images/right-fade.png\"\n\
}\n\
return \"images/right.png\"\n\
}\n\n\
function hc_fadeDown(){\n\
if(hc_curY == 0){\n\
	return \"images/down-fade.png\"\n\
}\n\
return \"images/down.png\"\n\
}\n\
\n\
function hc_panLeft(){\n\
var panBy = (hc_curEndX -hc_curX)/hc_panXFactor;\n\
hc_curX +=panBy;\n\
hc_curEndX +=panBy;\n\
hc_xScale.domain(hc_curX ,hc_curEndX );\n\
hc_rootPanel.render();\n\
hc_headerPanel.render();\n\
}\n\
\n\
function hc_panRight(){\n\
var panBy = (hc_curEndX -hc_curX)/hc_panXFactor;\n\
if(hc_curX > 0){\n\
hc_curX -=panBy;\n\
hc_curEndX -=panBy;\n\
if(hc_curX <= 0){\n\
hc_curEndX += (hc_curX + panBy)\n\
hc_curX = 0;\n\
}\n\
hc_xScale.domain(hc_curX ,hc_curEndX );\n\
hc_rootPanel.render();\n\
hc_headerPanel.render();\n\
}\n\
}\n\
\n\
function hc_panUp(){\n\
var panBy = (hc_curEndY -hc_curY)/hc_panYFactor;\n\
hc_curY +=panBy;\n\
hc_curEndY += panBy;\n\
hc_yScale.domain(hc_curY ,hc_curEndY);\n\
hc_rootPanel.render();\n\
hc_headerPanel.render();\n\
}\n\
\n\
function hc_panDown(){\n\
var panBy = (hc_curEndY -hc_curY)/hc_panYFactor;\n\
if(hc_curY > 0){\n\
hc_curY -= panBy;\n\
hc_curEndY -= panBy;\n\
if(hc_curY< 0){\n\
hc_curEndY += (hc_curY + panBy);\n\
hc_curY = 0;\n\
}\n\
hc_yScale.domain(hc_curY ,hc_curEndY );\n\
hc_rootPanel.render();\n\
hc_headerPanel.render();\n\
}\n\
}\n\
\n\
function hc_zoomOut(){\n\
var newX = 0;\n\
var newY = 0;\n\
\n\
newX = hc_curEndX  + hc_curEndX*0.1;\n\
newY = hc_curEndY  + hc_curEndY*0.1;\n\
\n\
if(hc_curX < newX && isFinite(newX)){\n\
hc_curEndX = newX;\n\
hc_xScale.domain(hc_curX, hc_curEndX);\n\
}\n\
if(hc_curY < newY && isFinite(newY)){\n\
hc_curEndY = newY;\n\
hc_yScale.domain(hc_curY, hc_curEndY);\n\
}\n\
hc_rootPanel.render();\n\
}\n\
\n\
function hc_zoomIn(){\n\
var newX = 0;\n\
var newY = 0;\n\
newX = hc_curEndX  - hc_curEndX*0.1;\n\
newY = hc_curEndY  - hc_curEndY*0.1;\n\
if(hc_curX < newX && isFinite(newX)){\n\
hc_curEndX = newX;\n\
hc_xScale.domain(hc_curX, hc_curEndX);\n\
}\n\
if(hc_curY < newY && isFinite(newY)){\n\
hc_curEndY = newY;\n\
hc_yScale.domain(hc_curY, hc_curEndY);\n\
}\n\
hc_rootPanel.render();\n\
}\n\
\n\
function hc_resetZomming(){\n\
hc_curX  = 0;\n\
hc_curY = 0;\n\
hc_curEndX  = hc_initMaxX;\n\
hc_curEndY =  hc_initMaxY;\n\
hc_xScale.domain(hc_curX, hc_curEndX);\n\
hc_yScale.domain(hc_curY, hc_curEndY);\n\
hc_rootPanel.render();\n\
hc_headerPanel.render();\n\
}\n"
	data_file = os.path.join(output_dir,  "hc_action.js")
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
#host_chart{
border:2px solid orange;
}
#host_chart_footer_div{
border:2px solid #C35617;
border-top-style:none;
}
#host_chart_legend_div{
color:#0066CC;
}
.header_level1{
font-family:"Times New Roman", Times, serif; 
font-size:36px;
}
.header_level2{
font-family:"Times New Roman", Times, serif; 
font-size:30px;
padding-top:25px;
}
</style>
</head>
<body>
<script type='text/javascript' src='js/protovis-r3.2.js'></script>
	"""
	header_str += plot_utils.create_home_button()
	return header_str
	
def create_toc(workflow_stat):
	"""
	Generates the table of content for the pages
	@param workflow_stat the WorkflowInfo object reference 
	"""
	toc_str ="""
<div class ='header_level1'>Workflow host over time chart </div>
	"""
	toc_str += """
<a href ='#chart_div'>Workflow host over time chart</a><br/>
<a href ='#env_div'> Workflow environment</a><br/>
	"""
	if len(workflow_stat.sub_wf_id_uuids) >0:
		toc_str += """
<a href ='#sub_div'> Sub workflows</a><br/>
""" 
	return toc_str
	
def create_include(workflow_stat):
	"""
	Generates the html script include content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	include_str = "\n\
<script type='text/javascript' src='hc_action.js'></script>\n\
<script type='text/javascript' src='hc_" + workflow_stat.wf_uuid  +"_data.js'></script>\n"
	return include_str
	
def create_variable(workflow_stat):
	"""
	Generates the javascript variables used to generate the chart.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	number_of_hosts = len(workflow_stat.host_job_map)
	# Adding  variables
	var_str = "<script type='text/javascript'>\nvar hc_initMaxX = " + str(workflow_stat.workflow_run_time) + ";\n"
	var_str +="var hc_bar_spacing = 20;\n\
var hc_inner_bar_margin = 4;\n\
var hc_line_width =2;\n\
var hc_inner_bar_width = hc_bar_spacing-2*hc_inner_bar_margin;\n\
var hc_nameMargin  = 400;\n\
var hc_scaleMargin = 15;\n"
	var_str += "var hc_initMaxY = "+str(number_of_hosts) + "*hc_bar_spacing;\n"
	color_name_str = "var hc_color =['yellow','orange' ,'steelblue'"
	desc_name_str = "var hc_desc=['condor job','resource delay', 'job runtime as seen by dagman'"
	for k,v in workflow_stat.transformation_color_map.items():
		if k in workflow_stat.transformation_statistics_dict:
			color_name_str += ",'"+v +"'"
			desc_name_str +=",'"+k +"'"	
	color_name_str += "];\n"
	desc_name_str +="];\n"
	var_str += color_name_str
	var_str += desc_name_str
	if number_of_hosts < 5:
		var_str +="var hc_h = " +str(number_of_hosts) +"*hc_bar_spacing*3 + hc_scaleMargin + hc_bar_spacing;\n"
	else:
		var_str +="var hc_h = 840;\n"			
	var_str +="var hc_w = 1460;\n\
var hc_toolbar_width = 550;\n\
var hc_containerPanelPadding = 20;\n\
var hc_chartPanelWidth = hc_w+ hc_containerPanelPadding*2;\n\
var hc_chartPanelHeight  = hc_h + hc_containerPanelPadding*2;\n\
var hc_curX  = 0;\n\
var hc_curY = 0;\n\
var hc_curEndX  = hc_initMaxX;\n\
var hc_curEndY =  hc_initMaxY;\n\
var hc_xScale = pv.Scale.linear(hc_curX, hc_curEndX).range(0, hc_w-hc_nameMargin);\n\
var hc_yScale = pv.Scale.linear(hc_curY, hc_curEndY).range(0, hc_h -hc_scaleMargin);\n\
var hc_xLabelPos = hc_containerPanelPadding + hc_nameMargin;\n\
var hc_LabelWidth = 200;\n\
var hc_yLabelPos = 40;\n\
var hc_panXFactor = 10;\n\
var hc_panYFactor  = 10;\n\
var hc_isNewWindow = false;\n\
var hc_condorTime = false;\n\
var hc_kickstart = false;\n\
var hc_condorRunTime = false;\n\
var hc_resourceDelay = false;\n\
var hc_showName = true;\n\
var hc_headerPanelWidth = hc_w+ hc_containerPanelPadding*2;\n\
var hc_headerPanelHeight  = 100;\n\
var hc_footerPanelWidth = hc_w+ hc_containerPanelPadding*2;\n\
var hc_footerPanelHeight  = "+ str(45 + len(workflow_stat.transformation_statistics_dict)/3*15) + ";\n\
</script>\n"
	return var_str
	

def create_toolbar_panel(workflow_stat, extn):
	"""
	Generates the top level toolbar content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	panel_str = "<script type=\"text/javascript+protovis\">\n\
var hc_headerPanel = new pv.Panel()\n\
.width(hc_headerPanelWidth)\n\
.height(hc_headerPanelHeight)\n\
.fillStyle('white');\n\n\
var hc_panPanel  = hc_headerPanel.add(pv.Panel)\n\
.left(hc_w + hc_containerPanelPadding -hc_toolbar_width)\n\
.width(hc_toolbar_width)\n\
.height(hc_headerPanelHeight);\n\n\
hc_panPanel.add(pv.Image)\n\
.left(10)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.title('Pan left')\n\
.url('images/left.png').event('click', hc_panLeft);\n\n\
hc_panPanel.add(pv.Image)\n\
.left(50)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.url(hc_fadeRight)\n\
.title('Pan right')\n\
.event('click', hc_panRight);\n\n\
hc_panPanel.add(pv.Image)\n\
.left(90)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.url('images/up.png')\n\
.title('Pan up')\n\
.event('click', hc_panUp);\n\
 hc_panPanel.add(pv.Image)\n\
.left(140)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.url(hc_fadeDown)\n\
.title('Pan down')\n\
.event('click', hc_panDown);\n\n\
hc_panPanel.add(pv.Image)\n\
.left(190)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.url('images/zoom-in.png')\n\
.title('Zoom in')\n\
.event('click', hc_zoomIn);\n\n\
hc_panPanel.add(pv.Image)\n\
.left(240)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.url('images/zoom-out.png')\n\
.title('Zoom out')\n\
.event('click', hc_zoomOut);\n\n\
hc_panPanel.add(pv.Image)\n\
.left(290)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.url('images/zoom-reset.png')\n\
.title('Zoom reset')\n\
.event('click', hc_resetZomming);\n\n\
hc_panPanel.add(pv.Image)\n\
.left(340)\n\
.top(34)\n\
.width(32)\n\
.height(32)\n\
.url(function() {if(hc_isNewWindow){ return 'images/new-window-press.png';}else{ return 'images/new-window.png';}})\n\
.title('Open sub workflow in new window')\n\
.event('click', function(){ if(hc_isNewWindow){ hc_isNewWindow = false;hc_headerPanel.render();}else{ hc_isNewWindow = true;hc_headerPanel.render();}});\n\n\
hc_panPanel.def('active', false);\n\n\
hc_panPanel.add(pv.Bar)\n\
.events('all')\n\
.left(390)\n\
.top(40)\n\
.width(100)\n\
.height(24)\n\
.event('click', hc_setShowName)\n\
.fillStyle(function() this.parent.active() ? 'orange' : '#c5b0d5')\n\
.strokeStyle('black')\n\
.event('mouseover', function() this.parent.active(true))\n\
.event('mouseout', function() this.parent.active(false))\n\
.anchor('left').add(pv.Label)\n\
	.textAlign('left')\n\
	.textMargin(5)\n\
	.textStyle(function() this.parent.active() ? 'white' : 'black')\n\
	.textBaseline('middle')\n\
	.text(hc_setShowLabel);\n\n"
	if workflow_stat.parent_wf_uuid is not None:
		panel_str += "hc_panPanel.add(pv.Image)\n.left(500)\n.top(34)\n.width(32)\n.height(32)\n.url('images/return.png')\n.title('Return to parent workflow')\n.event('click', function(){\nself.location = '" +  workflow_stat.parent_wf_uuid+"."+extn+"' ;\n});"
	panel_str += "hc_headerPanel.add(pv.Label)\n\
.top(40)\n\
.left( hc_containerPanelPadding + hc_nameMargin)\n\
.font(function() {return 24 +'px sans-serif';})\n\
.textAlign('left')\n\
.textBaseline('bottom')\n\
.text('Host Over Time Chart');\n\n\
hc_headerPanel.add(pv.Label)\n\
.top(80)\n\
.left(hc_containerPanelPadding + hc_nameMargin)\n\
.font(function() {return 16 +'px sans-serif';})\n\
.textAlign('left')\n\
.textBaseline('bottom')\n\
.text('"+workflow_stat.dax_label +"');\n\
hc_headerPanel.render();\n\n\
</script>\n"
	return panel_str

def create_chart_panel(workflow_stat):
	"""
	Generates the chart panel content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	panel_str ="<script type=\"text/javascript+protovis\">\n\
var hc_rootPanel = new pv.Panel()\n\
.width(hc_chartPanelWidth)\n\
.height(hc_chartPanelHeight)\n\
.fillStyle('white');\n\n\
var hc_vis = hc_rootPanel.add(pv.Panel)\n\
.bottom(hc_containerPanelPadding)\n\
.top(hc_containerPanelPadding)\n\
.left(hc_containerPanelPadding)\n\
.width(hc_w)\n\
.height(hc_h)\n\
.fillStyle('white');\n\n\
var rulePanelH = hc_vis.add(pv.Panel)\n\
.overflow('hidden')\n\
.bottom(hc_scaleMargin);\n\n\
rulePanelH.add(pv.Rule)\n\
.left(hc_nameMargin)\n\
.data(hc_data)\n\
.strokeStyle('#F8F8F8')\n\
.width(hc_w)\n\
.bottom( function(){\n\
return hc_yScale(this.index * hc_bar_spacing);\n\
})\n\
.anchor('left').add(pv.Label)\n\
.textBaseline('bottom')\n\
.text(function(d) {\n\
	if(hc_showName){\n\
	return (this.index +1) +' ' + d.name;\n\
	}else{\n\
		return (this.index +1) ;\n\
	}\n\
	});\n\n\
var rulePanelV = hc_vis.add(pv.Panel)\n\
.overflow('hidden')\n\
.left(hc_nameMargin)\n\
.bottom(0);\n\n\
rulePanelV.add(pv.Rule)\n\
.bottom(hc_scaleMargin)\n\
.data(function() hc_xScale.ticks())\n\
.strokeStyle('#F8F8F8')\n\
.left(hc_xScale)\n\
.height(hc_h )\n\
.anchor('bottom').add(pv.Label)\n\
.textAlign('left')\n\
.text(hc_xScale.tickFormat);\n\n\
var hc_chartPanelContainer = hc_vis.add(pv.Panel)\n\
.left(hc_nameMargin)\n\
.bottom(hc_scaleMargin)\n\
.width(hc_w-hc_nameMargin)\n\
.strokeStyle('black')\n\
.overflow('hidden');\n\n\
var hc_barPanel = hc_chartPanelContainer.add(pv.Panel)\n\
.data(hc_data)\n\
.height(function(){return hc_bar_spacing;})\n\
.bottom(function(d){\n\
return hc_yScale(this.index * hc_bar_spacing);});\n\n\
hc_barPanel.add(pv.Bar)\n\
.events('all')\n\
.data(function(d) d.jobs)\n\
.visible(function(d){return hc_barvisibility(d.jobS , this.parent.index);})\n\
.width(function(d) {\n\
return hc_getJobTime(d);})\n\
.left(function(d){\n\
if(!d.jobS){\n\
return 0;\n\
}\n\
return hc_xScale(d.jobS);} )\n\
.title(function(d)d.name)\n\
.event('click', function(d){\n\
	if(d.sub_wf){\n\
		hc_openWF(d.sub_wf_name);\n\
	}\n\
	else{\n\
	hc_printJobDetails(d);}\n\
})\n\
.fillStyle(function(d) {return 'white';})\n\
.lineWidth(hc_line_width)\n\
.strokeStyle(function(d) {return hc_getJobBorder(d);});\n\n\
hc_barPanel.add(pv.Bar)\n\
.data(function(d) d.jobs)\n\
.visible(function(d){return !hc_showState() && hc_barvisibility(d.jobS , this.parent.index);})\n\
.height(function(){return hc_inner_bar_width;})\n\
.bottom(hc_inner_bar_margin)\n\
.width(function(d) {\n\
return hc_getJobTime(d);})\n\
.left(function(d){\n\
if(!d.jobS){\n\
return 0;\n\
}\n\
return hc_xScale(d.jobS);} )\n\
.title(function(d)d.name)\n\
.event('click', function(d){\n\
	if(d.sub_wf){\n\
		hc_openWF(d.sub_wf_name);\n\
	}\n\
	else{\n\
	hc_printJobDetails(d);}\n\
})\n\
.fillStyle(function(d) {return d.color;});\n\n\
hc_barPanel.add(pv.Bar)\n\
.data(function(d) d.jobs)\n\
.visible(function(d){return hc_condorTime && hc_showState() && hc_barvisibility(d.cS , this.parent.index);})\n\
.height(function(){return hc_inner_bar_width;})\n\
.bottom(hc_inner_bar_margin)\n\
.width(function(d) {\n\
return hc_getCondorTime(d);})\n\
.left(function(d){\n\
if(!d.cS){\n\
return 0;\n\
}\n\
return hc_xScale(d.cS);} )\n\
.title(function(d)d.name)\n\
.event('click', function(d){\n\
	if(d.sub_wf){\n\
		hc_openWF(d.sub_wf_name);\n\
	}\n\
	else{\n\
 	hc_printJobDetails(d);}\n\
 	})\n\
.fillStyle(function(d) {return hc_color[0];});\n\n\
hc_barPanel.add(pv.Bar)\n\
.data(function(d) d.jobs)\n\
.visible(function(d){return hc_resourceDelay && hc_showState() && hc_barvisibility(d.gS , this.parent.index);})\n\
.height(function(){return hc_inner_bar_width;})\n\
.bottom(hc_inner_bar_margin)\n\
.width(function(d) {\n\
return hc_getResourceDelay(d);})\n\
.left(function(d){\n\
if(!d.gS){\n\
return 0;\n\
}\n\
return hc_xScale(d.gS);} )\n\
.title(function(d)d.name)\n\
.event('click', function(d){\n\
	if(d.sub_wf){\n\
	hc_openWF(d.sub_wf_name);\n\
	}\n\
	else{\n\
	hc_printJobDetails(d);}\n\
	})\n\
.fillStyle(function(d) {return hc_color[1];});\n\n\
hc_barPanel.add(pv.Bar)\n\
.data(function(d) d.jobs)\n\
.visible(function(d){return hc_condorRunTime && hc_showState() && hc_barvisibility(d.eS , this.parent.index);})\n\
.height(function(){return hc_inner_bar_width;})\n\
.bottom(hc_inner_bar_margin)\n\
.width(function(d) {\n\
return hc_getRunTime(d);})\n\
.left(function(d){\n\
if(!d.eS){\n\
return 0;\n\
}\n\
return hc_xScale(d.eS);} )\n\
.title(function(d)d.name)\n\
.event('click', function(d){\n\
	if(d.sub_wf){\n\
		hc_openWF(d.sub_wf_name);\n\
	}\n\
	else{\n\
 	hc_printJobDetails(d);}\n\
 	})\n\
.fillStyle(function(d) {return hc_color[2];});\n\n\
hc_barPanel.add(pv.Bar)\n\
.data(function(d) d.jobs)\n\
.visible(function(d){return hc_kickstart && hc_showState() && hc_barvisibility(d.kS , this.parent.index);})\n\
.height(function(){return hc_inner_bar_width;})\n\
.bottom(hc_inner_bar_margin)\n\
.width(function(d) {\n\
return hc_getKickStartTime(d);})\n\
.left(function(d){\n\
if(!d.kS){\n\
return 0;\n\
}\n\
return hc_xScale(d.kS);} )\n\
.title(function(d)d.name)\n\
.event('click', function(d){\n\
	if(d.sub_wf){\n\
		hc_openWF(d.sub_wf_name);\n\
	}\n\
	else{\n\
 	hc_printJobDetails(d);}\n\
 	})\n\
.fillStyle(function(d) {return d.color;});\n\n\
hc_rootPanel.add(pv.Label)\n\
.bottom(hc_containerPanelPadding)\n\
.font(function() {return 20 +'px sans-serif';})\n\
.textAlign('left')\n\
.textBaseline('top')\n\
.text('Host count -->')\n\
.textAngle(-Math.PI / 2);\n\n\
hc_rootPanel.add(pv.Label)\n\
.left(hc_containerPanelPadding + hc_nameMargin)\n\
.bottom(0)\n\
.font(function() {return 20 +'px sans-serif';})\n\
.textAlign('left')\n\
.textBaseline('bottom')\n\
.text('Timeline in seconds -->');\n\n\
hc_rootPanel.render();\n\
</script>\n"
	return panel_str
	

def create_legend_panel(workflow_stat):
	"""
	Generates the bottom level legend panel content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	panel_str ="<script type=\"text/javascript+protovis\">\n\
var hc_footerpanel = new pv.Panel()\n\
.width(hc_footerPanelWidth)\n\
.height(hc_footerPanelHeight)\n\
.fillStyle('white');\n\n\
hc_footerpanel.add(pv.Dot)\n\
.data(hc_desc)\n\
.left( function(d){\n\
if(this.index == 0){\n\
hc_xLabelPos = hc_containerPanelPadding + hc_nameMargin;\n\
hc_yLabelPos = hc_footerPanelHeight -15;\n\
}else{\n\
if(hc_xLabelPos + hc_LabelWidth > hc_w - ( hc_containerPanelPadding + hc_nameMargin )){\n\
	hc_xLabelPos =  hc_containerPanelPadding + hc_nameMargin;\n\
	hc_yLabelPos -=15;\n\
}\n\
else{\n\
hc_xLabelPos += hc_LabelWidth;\n\
}\n\
}\n\
return hc_xLabelPos;}\n\
)\n\
.bottom(function(d){\n\
return hc_yLabelPos;})\n\
.fillStyle(function(d) hc_color[this.index])\n\
.strokeStyle(null)\n\
.size(49)\n\
.anchor('right').add(pv.Label)\n\
.textMargin(6)\n\
.textAlign('left')\n\
.text(function(d) d);\n\n\
hc_footerpanel.render();\n\n\
</script>\n"
	return panel_str

def create_bottom_toolbar():
	"""
	Generates the bottom toolbar html content.
	@param workflow_stat the WorkflowInfo object reference 
	"""
	toolbar_content ="""
<div id ='host_chart_footer_div' style='width: 1500px; margin : 0 auto;' >
<img style='float: right' src = 'images/jobstates.png'/>
<input type='checkbox' name='state' value='show condor job' onclick="hc_setCondorTime();" /> show condor job [JOB_TERMINATED -SUBMIT]<br />
<input type='checkbox' name='state' value='kickstart' onclick="hc_setKickstart();"/> show kickstart time <br />
<input type='checkbox' name='state' value='execute'   onclick="hc_setCondorRuntime();"/> show runtime as seen by dagman [JOB_TERMINATED - EXECUTE]<br />
<input type='checkbox' name='state' value='resource'  onclick="hc_setResourceDelay();"/> show resource delay  [EXECUTE -GRID_SUBMIT/GLOBUS_SUBMIT] <br/>
<div id = 'host_chart_legend_div'>
	<p><b>Note</b>: Sub workflow job instances are drawn with orange border and clicking on the sub workflow job instance <br/>
	      will take you to the sub workflow chart page. Failed job instances are drawn with red border. Clicking on a non <br/>
	 	  sub workflow job instance will display the job instance information. Mouse over the bars will provided the job <br/>  
	 	  names. Host names are marked 'Unknown' when the host name cannot be resolved by the pegasus system.
	 </p>
</div>
<div style='clear: right'></div>
</div><br/>
	""" 
	return toolbar_content

def create_host_plot(workflow_info,output_dir , extn ="html"):
	"""
	Generates the html page content for displaying the host chart.
	@param workflow_stat the WorkflowInfo object reference 
	@output_dir the output directory path
	"""
	print_workflow_details(workflow_info ,output_dir, extn)
	str_list = []
	wf_content = create_include(workflow_info)
	str_list.append(wf_content)
	# Adding  variables
	wf_content =create_variable(workflow_info )
	str_list.append(wf_content)
	# adding the tool bar panel
	wf_content = "<div id ='host_chart' style='width: 1500px; margin : 0 auto;' >\n"
	str_list.append(wf_content)
	wf_content =create_toolbar_panel(workflow_info , extn)
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

def create_host_plot_page(workflow_info , output_dir , extn ="html"):
	"""
	Prints the complete html page with the host chart and workflow details.
	@param workflow_stat the WorkflowInfo object reference 
	@output_dir the output directory path
	"""	
	str_list = []
	wf_page = create_header(workflow_info)
	str_list.append(wf_page)
	wf_page = """
<center>
	"""
	str_list.append(wf_page)
	wf_page = create_toc(workflow_info)
	str_list.append(wf_page)
	wf_page = """<div id ='chart_div' class ='header_level2'> Host over time chart </div>"""
	str_list.append(wf_page)
	wf_page = create_host_plot(workflow_info , output_dir ,extn)
	str_list.append(wf_page)
	# printing the brain dump content
	wf_page = """<div id ='env_div' class ='header_level2'> Workflow environment </div>"""
	str_list.append(wf_page)
	if workflow_info.submit_dir is None:
		logger.warning("Unable to display brain dump contents. Invalid submit directory for workflow  " + workflow_info.wf_uuid)
	else:
		wf_page = plot_utils.print_property_table(workflow_info.wf_env,False ," : ")
		str_list.append(wf_page)
	# print sub workflow list
	if len(workflow_info.sub_wf_id_uuids) >0:
		wf_page = """<div id ='sub_div' class ='header_level2'> Sub workflows </div>"""
		str_list.append(wf_page)
		wf_page = plot_utils.print_sub_wf_links(workflow_info.sub_wf_id_uuids)
		str_list.append(wf_page)
	wf_page = """
</center>
	"""
	str_list.append(wf_page)
	wf_page = """
<div style='clear: left'>
</div>
</body>
</html>
	"""
	str_list.append(wf_page)
	# print html file
	data_file = os.path.join(output_dir,  workflow_info.wf_uuid+"."+extn)
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
	
def setup(submit_dir,out_dir,env):
	"""
	Setup the pegasus host over time module
	@param submit_dir submit directory path 
	@out_dir the output directory path
	@env the environment variables
	"""
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
	Generates the host chart and all it's required files
	@workflow_info WorkflowInfo object reference
	"""
	create_host_plot_page(workflow_info,output_dir )


