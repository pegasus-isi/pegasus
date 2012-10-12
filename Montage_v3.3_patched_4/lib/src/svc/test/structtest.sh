#!/bin/sh

structtest << EOF
[struct 
   cmd = "pi",
   stat="OK",
   center = [struct 
      pixel_value = 297 ,
      units = " DN",
      coord = [struct 
	 coord_sys = "screen",
	 x = "100",
	 y = [array 
	    100,
	    "      x      ",
	    "",
	    234.333,
	    "abc def"
	 ]
      ],
      coefficients = [array
	 [struct A=0.305, B=0.764, C=0.163, D=0.002],
	 [struct A=0.852, B=0.293, C=0.841, D=0.004],
	 [struct A=0.274, B=0.516, C=0.565, D=0.001],
	 [struct A=0.747, B=0.457, C=0.454, D=0.003],
	 [struct A=0.815, B=0.372, C=0.648, D=0.001]
      ]
   ],
   pixel_count = 1,
   max_pixel_value = 2975,
   frame = 1,
   filename = "/home/jcg/ki0260032.fits"
]

stat
pixel_count
filename
center.coefficients[2].B
center.coord.x
center.coord.y[1]

EOF
