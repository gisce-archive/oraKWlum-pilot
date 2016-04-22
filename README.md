# oraKWlum
An energy forecasting lib based on enerdata

Predicts future consumptions based on a previous (already known) scenario

Interact with Mongo to perform the forecasts and store the related documents

Can import F1, Q1 and P5D files.

Proposals can be viewed using [oraKWlum-frontend](https://github.com/gisce/oraKWlum-frontend)


## Example of use

See [example](https://github.com/gisce/oraKWlum/blob/master/example.py)
and [output](https://github.com/gisce/oraKWlum/blob/master/example_stdout.txt)


## Generated reports

```
     h     	'Original projection'	'CUPS increased'	'CUPS erased'	'Margin +15%'	
01/03 00:00	        1176         	     1212.0     	   1139.0    	   1293.6    	
01/03 01:00	        1331         	     1377.0     	   1278.0    	   1464.1    	
01/03 02:00	        1340         	     1447.0     	   1256.0    	   1474.0    	
01/03 03:00	        1125         	     1158.0     	   1082.0    	   1237.5    	
01/03 04:00	        1432         	     1452.0     	   1382.0    	   1575.2    	
01/03 05:00	        1298         	     1364.0     	   1218.0    	   1427.8    	
01/03 06:00	        1357         	     1400.0     	   1324.0    	   1492.7    	
01/03 07:00	        1324         	     1384.0     	   1275.0    	   1456.4    	
01/03 08:00	        1424         	     1529.0     	   1369.0    	   1566.4    	
01/03 09:00	        1380         	     1478.0     	   1326.0    	   1518.0    	
01/03 10:00	        1308         	     1394.0     	   1230.0    	   1438.8    	
01/03 11:00	        1199         	     1280.0     	   1100.0    	   1318.9    	
01/03 12:00	        1319         	     1401.0     	   1300.0    	   1450.9    	
01/03 13:00	        1121         	     1182.0     	   1068.0    	   1233.1    	
01/03 14:00	        1146         	     1167.0     	   1106.0    	   1260.6    	
01/03 15:00	        1187         	     1264.0     	   1150.0    	   1305.7    	
01/03 16:00	        1125         	     1195.0     	   1033.0    	   1237.5    	
01/03 17:00	        1341         	     1409.0     	   1313.0    	   1475.1    	
01/03 18:00	        1283         	     1317.0     	   1244.0    	   1411.3    	
01/03 19:00	        1234         	     1289.0     	   1190.0    	   1357.4    	
01/03 20:00	        1236         	     1324.0     	   1226.0    	   1359.6    	
01/03 21:00	        1271         	     1363.0     	   1205.0    	   1398.1    	
01/03 22:00	        1317         	     1425.0     	   1235.0    	   1448.7    	
01/03 23:00	        1432         	     1522.0     	   1404.0    	   1575.2    	
02/03 00:00	        1303         	     1413.0     	   1289.0    	   1433.3    	
```

<table> <thead> <tr> <th>h</th><th>Original projection</th><th>CUPS increased</th><th>CUPS erased</th><th>Margin +15%</th></tr></thead>
<tbody><tr><td>01/03 00:00</td><td>1176</td><td>1212.0</td><td>1139.0</td><td>1293.6</td></tr><tr><td>01/03 01:00</td><td>1331</td><td>1377.0</td><td>1278.0</td><td>1464.1</td></tr><tr><td>01/03 02:00</td><td>1340</td><td>1447.0</td><td>1256.0</td><td>1474.0</td></tr><tr><td>01/03 03:00</td><td>1125</td><td>1158.0</td><td>1082.0</td><td>1237.5</td></tr><tr><td>01/03 04:00</td><td>1432</td><td>1452.0</td><td>1382.0</td><td>1575.2</td></tr><tr><td>01/03 05:00</td><td>1298</td><td>1364.0</td><td>1218.0</td><td>1427.8</td></tr><tr><td>01/03 06:00</td><td>1357</td><td>1400.0</td><td>1324.0</td><td>1492.7</td></tr><tr><td>01/03 07:00</td><td>1324</td><td>1384.0</td><td>1275.0</td><td>1456.4</td></tr><tr><td>01/03 08:00</td><td>1424</td><td>1529.0</td><td>1369.0</td><td>1566.4</td></tr><tr><td>01/03 09:00</td><td>1380</td><td>1478.0</td><td>1326.0</td><td>1518.0</td></tr><tr><td>01/03 10:00</td><td>1308</td><td>1394.0</td><td>1230.0</td><td>1438.8</td></tr><tr><td>01/03 11:00</td><td>1199</td><td>1280.0</td><td>1100.0</td><td>1318.9</td></tr><tr><td>01/03 12:00</td><td>1319</td><td>1401.0</td><td>1300.0</td><td>1450.9</td></tr><tr><td>01/03 13:00</td><td>1121</td><td>1182.0</td><td>1068.0</td><td>1233.1</td></tr><tr><td>01/03 14:00</td><td>1146</td><td>1167.0</td><td>1106.0</td><td>1260.6</td></tr><tr><td>01/03 15:00</td><td>1187</td><td>1264.0</td><td>1150.0</td><td>1305.7</td></tr><tr><td>01/03 16:00</td><td>1125</td><td>1195.0</td><td>1033.0</td><td>1237.5</td></tr><tr><td>01/03 17:00</td><td>1341</td><td>1409.0</td><td>1313.0</td><td>1475.1</td></tr><tr><td>01/03 18:00</td><td>1283</td><td>1317.0</td><td>1244.0</td><td>1411.3</td></tr><tr><td>01/03 19:00</td><td>1234</td><td>1289.0</td><td>1190.0</td><td>1357.4</td></tr><tr><td>01/03 20:00</td><td>1236</td><td>1324.0</td><td>1226.0</td><td>1359.6</td></tr><tr><td>01/03 21:00</td><td>1271</td><td>1363.0</td><td>1205.0</td><td>1398.1</td></tr><tr><td>01/03 22:00</td><td>1317</td><td>1425.0</td><td>1235.0</td><td>1448.7</td></tr><tr><td>01/03 23:00</td><td>1432</td><td>1522.0</td><td>1404.0</td><td>1575.2</td></tr><tr><td>02/03 00:00</td><td>1303</td><td>1413.0</td><td>1289.0</td><td>1433.3</td></tr></tbody></table>

