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
    
    $(selector + '> tbody > tr').each (function (index)
				       {
					   if (status != null && status == 'Failed')
					   {
					       $(this).addClass ('failed'); // Might require change in some CSS file, probably jquery*dataTables*theme*.css file.
					   }
					   else if (status != null && status == 'Successful')
                                           {
                                               $(this).addClass ('failed');
                                           }
					   else
					   {
					       $(this).addClass ('running');
					   }
				       });
}
