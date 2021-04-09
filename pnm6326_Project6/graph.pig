E = LOAD '$G' using PigStorage(',') as (x:int, y:int );

K = GROUP E by x;
Neighbour_count = FOREACH K generate group, COUNT(E.$1);
Group_by_neighbour_count = GROUP Neighbour_count by $1;

O = FOREACH Group_by_neighbour_count GENERATE group , COUNT(Neighbour_count.$1) ;
STORE O INTO '$O' using PigStorage (',');
