"use strict";

$(function () {
    $('[data-toggle="popover"]').popover();
});

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

	var isOdd = true;

	$(selector + '> tbody > tr').each (function (index)
	{
		if (isOdd)
    	{
    		$(this).addClass ('odd');
    	}
    	else
    	{
    		$(this).addClass ('even');
    	}

    	isOdd = !isOdd;
	});
}

/**
 * Display a table with based on status
 */
function verticalTableInitStatus (selector, status)
{
    var table = $(selector);
    table.addClass ('dataTable');
    table.attr ('cellspacing', 0);
    table.attr ('cellpadding', 5);
    $(selector + '> tbody > tr > th').each (function (index)
    {
    	$(this).addClass ('ui-state-default nowrap');
    });

    var isOdd = true;

    $(selector + '> tbody > tr').each (function (index)
    {
    	if (isOdd)
    	{
    		$(this).addClass ('odd');
    	}
    	else
    	{
    		$(this).addClass ('even');
    	}

    	isOdd = !isOdd;

    	if (status != null && status.toLowerCase()  == 'failed')
    	{
    		$(this).addClass ('failed');
    	}
    	else if (status != null && status.toLowerCase()  == 'successful')
    	{
    		$(this).addClass ('successful');
    	}
    	else
    	{
    		$(this).addClass ('running');
    	}
   });
}
