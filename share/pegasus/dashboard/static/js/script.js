"use strict";

function verticalTableInit (selector)
{
	var table = $(selector);
	table.addClass ('dataTable');
	table.attr ('cellspacing', 0);
	table.attr ('cellpadding', 5);
	$(selector + '> tbody > tr > th').each (function (index)
	{
		$(this).addClass ('ui-state-default nowrap');
	});
	
	$(selector + '> tbody > tr').each (function (index)
	{
		$(this).addClass ('odd');
	});
}