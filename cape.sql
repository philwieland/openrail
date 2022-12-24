DROP TABLE IF EXISTS cape_reasons;
CREATE TABLE cape_reasons ( code CHAR(2) NOT NULL, reason VARCHAR(256) NOT NULL) Engine = InnoDB;
INSERT INTO  cape_reasons VALUES
('AA','Waiting Terminal/Yard acceptance'), 
('AB','Waiting Customer release of documentation'), 
('AC','Waiting train preparation or completion of TOPS list/RT3973'), 
('AD','Terminal/Yard staff shortage including reactionary congestion caused by the shortage'), 
('AE','Congestion in Terminal/Yard'), 
('AF','Terminal/Yard equipment failure - cranes etc'), 
('AG','Adjusting Loaded wagons'), 
('AH','Customer equipment breakdown/reduced capacity'), 
('AJ','Waiting Customer\'s traffic including ship/road/air connections and Mail deliveries.'), 
('AK','Fire in freight yard / terminal (including private sidings, and stations - where it affects FOC services)'), 
('AX','Failure of FOC-owned infrastructure'), 
('AY','Mishap in Terminal/Yard or on Terminal/Yard infrastructure'), 
('AZ','Other Freight Operating Company, cause to be specified'), 
('FA','Dangerous goods incident'), 
('FB','Train stopped on route due to incorrect marshalling'), 
('FC','Freight train driver'), 
('FD','Booked loco used on additional/other service'), 
('FE','Traincrew rostering error/not available, including crew relief errors'), 
('FG','Driver adhering to company professional driving standards or policy'), 
('FH','Train crew/loco diagram/planning error (See also DAG Section 4.31.2)'), 
('FI','Delay in running due to the incorrect operation of the on-board ETCS/ ERTMS equipment i.e. wrong input by driver'), 
('FJ','Train held at Customer\'s request'), 
('FK','Train diverted/re-routed at Customer\'s request'), 
('FL','Train cancelled at Customer\'s request'), 
('FM','Tail lamp/head lamp out or incorrectly shown'), 
('FN','Late presentation from Europe'), 
('FO','Delay in running believed to be due to Operator, but no information available from that Operator'), 
('FP','Incorrect route taken or route wrongly challenged by driver, including SPADs'), 
('FS','Delay due to ETCS/ ERTMS on-board overriding driver command'), 
('FT','Freight Operator autumn-attribution Neutral Zone delays'), 
('FU','Formal Inquiry Incident - possible Operator responsibility'), 
('FW','Late start/yard overtime not explained by Operator'), 
('FX','Freight train running at lower than planned classification'), 
('FY','Mishap caused by Freight Operating Company or on FOC-owned infrastructure'), 
('FZ','Other Freight Operating Company causes, including Freight Operating Company Control directive, cause to be specified'), 
('I0','Telecom equipment failures legacy (inc. NRN/CSR/RETB link)'), 
('I1','Overhead line/third rail defect'), 
('I2','AC/DC trip'), 
('I3','Obstruction on OHL, cause of which is not known'), 
('I4','OHL/third rail power supply failure/reduction'), 
('I5','Possession over-run from planned work'), 
('I6','Track Patrolling'), 
('I7','Engineer\'s train late/failed in possession'), 
('I8','Animal Strike/Incursion within the control of Network Rail'), 
('I9','Fires starting on Network Rail Infrastructure'), 
('IA','Signal failure'), 
('IB','Points failure'), 
('IC','Track circuit failure'), 
('ID','Level crossing failure incl. barrow/foot crossings and crossing treadles'), 
('IE','Power failure'), 
('IF','Train Describer/Panel/ARS/SSI/TDM/Remote Control failure'), 
('IG','Block failure'), 
('II','Power Supply cable fault/fire due to cable fault'), 
('IJ','AWS/ATP/TPWS/Train Stop/On-track equipment failure'), 
('IK','Telephone equipment failure'), 
('IL','Token equipment failure'), 
('IM','Infrastructure Balise Failure'), 
('IN','HABD/Panchex/WILD/Wheelchex'), 
('IO','No fault found/HABD/Panchex/WILD/Wheelchex'), 
('IP','Points failure caused by snow or frost where heaters are fitted but found to be not operative or defective'), 
('IQ','Trackside sign blown down/light out etc'), 
('IR','Broken/cracked/twisted/buckled/flawed rail'), 
('IS','Track defects (other than rail defects i.e. fish plates, wet beds etc)'), 
('IT','Bumps reported - cause not known'), 
('IU','Engineers on-track plant affecting possession'), 
('IV','Earthslip/subsidence/breached sea defences not the result of severe weather'), 
('IW','Non severe- Snow/Ice/Frost affecting infrastructure equipment'), 
('IY','Mishap - Network Rail causes'), 
('IZ','Other infrastructure causes'), 
('J0','Telecom radio failures IVRS/GSM-R'), 
('J2','TRTS Failure'), 
('J3','Axle Counter Failure'), 
('J4','Safety Issue No Fault Found'), 
('J5','NR DOO monitor/mirror failure'), 
('J6','Lightning strike against unprotected assets'), 
('J7','ETCS/ ERTMS Equipment Failure (excluding communications link and balises)'), 
('J8','Damage to infrastructure caused by on-track machine whilst operating in a possession'), 
('JA','TSR speeds for Track-work outside the Rules of the Route'), 
('JB','Reactionary Delay to "P" coded TSR'), 
('JC','Telecom cable failure (transmission sys and cable failures )'), 
('JD','Bridges/tunnels/buildings (other than bridge strikes)'), 
('JG','ESR/TSR due to cancelled possession/work not completed'), 
('JH','Critical Rail Temperature speeds, (other than buckled rails)'), 
('JI','Swing/lifting bridge failure'), 
('JK','Flooding not due to exceptional weather'), 
('JL','Network Rail/TRC Staff error'), 
('JM','Change of Signal Aspects - no fault found'), 
('JN','Possession cancellation'), 
('JO','Rolling Contact Fatigue'), 
('JP','Failure to maintain vegetation within network boundaries in accordance with prevailing Network Rail standards'), 
('JQ','Trains striking overhanging branches/vegetation (not weather- related)'), 
('JR','Signals/track signs obscured by vegetation'), 
('JS','Condition of Track TSR Outside Rules of Route'), 
('JT','Points failure caused by snow or frost where heaters are not fitted'), 
('JX','Miscellaneous items on the track or railhead including litter, excluding leaves, demonstrated vandalism, weather or items fallen/thrown from trains'), 
('M0','Safety systems failure (DSD/OTMR/Vigilance)'), 
('M1','Pantograph fault or PANCHEX activation (positive)'), 
('M2','Automatic Dropper Device activation'), 
('M3','Diesel loco failure/defect/attention: other'), 
('M4','EMU failure/defect/attention: brakes'), 
('M5','EMU failure/defect/attention: doors (including SDO equipment failure)'), 
('M6','EMU failure/defect/attention: other'), 
('M7','DMU (inc. HST/MPV) failure/defect/attention: doors (including SDO equipment failure and excluding Railhead Conditioning trains).'), 
('M8','DMU (inc. HST/MPV) failure/defect/attention: other (excluding Railhead Conditioning trains)'), 
('M9','Reported fleet equipment defect - no fault found'), 
('MA','Electric loco (inc. IC225) failure/defect/attention: brakes'), 
('MB','Electric loco (inc. IC225) failure/defect/attention: traction'), 
('MC','Diesel loco failure/defect/attention: traction'), 
('MD','DMU (inc. HST)/MPV failure/defect/attention: traction (excluding Railhead Conditioning trains)'), 
('ME','Steam locomotive failure/defect/attention'), 
('MF','International/Channel Tunnel locomotive failure/defect/attention'), 
('MG','Coach (inc. Intl/IC225) failure/defect/attention: brakes'), 
('MH','Coach (inc. Intl/IC225) failure/defect/attention: doors'), 
('MI','Coach (inc. Intl/IC225) failure/defect/attention: other'), 
('MJ','Parcels vehicle failure/defect/attention'), 
('MK','DVT/PCV failure/defect/attention'), 
('ML','Freight vehicle failure/defect attention (inc. private wagons)'), 
('MM','EMU failure/defect/attention: traction'), 
('MN','DMU (inc. HST/MPV) failure/defect/attention: brakes (excluding Railhead Conditioning trains)'), 
('MO','Loco/unit/vehicles late off depot (cause not known)'), 
('MP','Loco/unit adhesion problems'), 
('MQ','Electric loco (inc. IC225) failure/defect/attention: other'), 
('MR','Hot Box or HABD/WILD activation (positive)'), 
('MS','Stock change or replacement by slower vehicles (all vehicle types)'), 
('MT','Safety systems failure (AWS/TPWS/ATP)'), 
('MU','Depot operating problem'), 
('MV','Engineer\'s on-track equipment failure outside possession'), 
('MW','Weather - effect on T and RS equipment'), 
('MX','Diesel loco failure/defect/attention: brakes'), 
('MY','Mishap - T and RS cause'), 
('MZ','Other Fleet Engineer causes/initial attribution'), 
('NA','Ontrain TASS Failure'), 
('NB','TASS - No fault found'), 
('NC','Fire in fleet depot not caused by vandals (includes caused by vandals in respect of freight depots)'), 
('ND','On train ETCS/ ERTMS failure'), 
('O2','ACI Failures'), 
('OB','Delayed by signaller not applying applicable regulating policy'), 
('OC','Signaller, including wrong routing and wrong ETCS/ ERTMS instruction'), 
('OD','Delays due to national, regional or route operations directives, or route control decisions or directive'), 
('OE','Failure to lay Sandite or operate Railhead Conditioning train as programmed'), 
('OG','Ice on conductor rail/OLE'), 
('OH','ARS software problem (excluding scheduling error and technical failures)'), 
('OI','Formal Inquiry Incident - other operators'), 
('OJ','Fire in station building or on platform, affecting operators not booked to call at that station'), 
('OK','Delay caused by Operating staff oversight, error or absence (excluding signallers and Control)'), 
('OL','Signal Box not open during booked hours'), 
('OM','Technical failure associated with a Railhead Conditioning train'), 
('ON','Delays not properly investigated by Network Rail'), 
('OO','Late start of a RHC'), 
('OP','Failure of TRUST/SMART systems'), 
('OQ','Incorrect Simplifier'),
('OR','LOM Directive or signaller correctly applying local box instructions'),
('OS','Delays to other trains caused by a Railhead Conditioning train taking unusually long time in section or at a location'), 
('OU','Delays un-investigated'), 
('OV','Fire or evacuation due to fire alarm of Network Rail buildings other than stations not due to vandalism'), 
('OW','Connections held where the prime incident causing delay to the incoming train is a FOC owned incident and service is more frequent than hourly'), 
('OY','Mishap - Network Rail Operating cause'), 
('OZ','Other Network Rail Operating causes'), 
('PA','Trackwork TSR within Rules of the Route'), 
('PB','Condition of Track TSR within Rules of the Route'), 
('PC','Condition of Bridge TSR within rules of the route'), 
('PD','TPS cancellation (Not to be input in TSI/TRUST)'), 
('PE','Cancelled due to planned engineering work'), 
('PF','Planned engineering work - diversion/SLW not timetabled (within Rules of the Route)'), 
('PG','Planned cancellation by Train Operator'), 
('PH','Condition of Earthworks TSR within Rules of the Route'), 
('PI','TSR for Schedule 4 Possession'), 
('PJ','Duplicate delay'), 
('PK','Bank Holiday Cancellation'), 
('PL','Exclusion agreed between Network Rail and Train Operator'), 
('PN','Minor delays to VSTP service caused by regulation / time lost in running.'), 
('PS','Cancellation of a duplicate or erroneous schedule'), 
('PT','TRUST Berth Offset Errors'), 
('PZ','Other contractual exclusion'), 
('QA','WTT Schedule / LTP process'), 
('QB','Planned engineering work - diversion/SLW not timetabled (outside rules of the route)'), 
('QH','Adhesion problems due to leaf contamination'), 
('QI','Cautioning due to railhead leaf contamination'), 
('QJ','Special working for leaf-fall track circuit operation'), 
('QM','STP schedule / STP process'), 
('QN','VSTP schedule / VSTP process (TSI created schedule)'), 
('QP','Reactionary Delay to "P" coded Possession'), 
('QQ','Simplifier Error Ops Planning'), 
('QT','Delay accepted by Network Rail as part of a commercial agreement where no substantive delay reason is identified'), 
('QZ','Other Network Rail non-Operating causes'), 
('R1','Incorrect train dispatch by station staff'), 
('R2','Late TRTS given by station staff'), 
('R3','Station staff unavailable - missing or uncovered'), 
('R4','Station staff split responsibility - unable to cover all duties'), 
('R5','Station staff error - e.g. wrong announcements, misdirection'), 
('R6','Overtime at stations normally unstaffed.'), 
('R7','Station delays due to special events e.g. sports fixtures'), 
('RB','Passengers joining/alighting'), 
('RC','Assisting a disabled person joining/alighting, pre-booked'), 
('RD','Attaching/detaching/shunter/watering'), 
('RE','Lift/escalator defect/failure'), 
('RF','Loading/unloading letter mails/parcels'), 
('RH','Station evacuated due to fire alarm'), 
('RI','Waiting connections - not authorised by TOC Control'), 
('RJ','Special Stop Orders - not authorised by TOC Control'), 
('RK','Waiting connections - authorised by TOC Control'), 
('RL','Special Stop Orders - authorised by TOC Control'), 
('RM','Waiting connections from other transport modes'), 
('RN','Passengers "forcing" connections between trains outside connectional allowances'), 
('RO','Passengers taken ill on platform'), 
('RP','Passengers dropping items on track (not vandalism)'), 
('RQ','Assisting a disabled person joining/alighting, unbooked'), 
('RR','Loading reserved bicycles presented late'), 
('RS','Loading unreserved bicycles'), 
('RT','Loading excessive luggage'), 
('RU','Locating lost luggage'), 
('RV','Customer Information System failure'), 
('RW','Station flooding (including issues with drains) not the result of weather, where the water has not emanated from Network Rail maintained infrastructure/network'), 
('RY','Mishap - Station Operator cause'), 
('RZ','Other Station Operator causes'), 
('T1','Delay at unstaffed station to DOO train'), 
('T2','Delay at unstaffed station to non-DOO train'), 
('T3','Waiting connections from other transport modes'), 
('T4','Loading Supplies (including catering)'), 
('TA','Traincrew/loco/stock/unit diagram error (See also DAG Section 4.31.2)'), 
('TB','Train cancelled/delayed at Train Operator\'s request'), 
('TC','Booked Traincrew used for additional/other service'), 
('TD','Booked loco/stock/unit used for additional/other service'), 
('TE','Injury to passenger on train'), 
('TF','Seat reservation problems'), 
('TG','Driver'), 
('TH','(Senior) Conductor/Train Manager'), 
('TI','Traincrew rostering problem'), 
('TJ','Tail lamp/headlamp out'), 
('TK','Train catering staff (including Contractors)'), 
('TL','Door open / not properly secured incident'), 
('TM','Connection authorised by TOC but outwith connection policy'), 
('TN','Late presentation from the continent'), 
('TO','Delay believed to be due to Operator, but no information available from Operator'), 
('TP','Special Stop Orders'), 
('TR','Train Operating Company Directive'), 
('TS','Delay due to ETCS/ ERTMS on-board overriding driver command'), 
('TT','Autumn-attribution Neutral Zone delays (See Supplementary Autumn Attribution Guidance)'), 
('TU','Formal Inquiry Incident - possible Operator responsibility'), 
('TW','Driver adhering to company professional driving standards or policy'), 
('TX','Delays incurred on non-Network Rail running lines including London Underground causes (except T and RS)'), 
('TY','Mishap-Train Operating Company cause'), 
('TZ','Other Passenger Train Operating Company causes'), 
('V8','Train striking other birds'), 
('VA','Disorder/drunks/trespass etc'), 
('VB','Vandalism/theft'), 
('VC','Fatalities/injuries sustained whilst on a platform as the result of being struck by a train or falling from a train'), 
('VD','Passenger taken ill on train'), 
('VE','Ticket irregularities/refusals to pay'), 
('VF','Fire caused by vandalism'), 
('VG','Police searching train'), 
('VH','Communication cord/emergency train alarm operated'), 
('VI','Security alert affecting stations and depots'), 
('VR','Driver adhering to company professional driving standards or policies during severe weather conditions that are not fleet related'), 
('VW','Severe weather affecting passenger fleet equipment including following company standards/policies or Rule Book instructions'), 
('VX','Passenger charter excludable events occurring on the LUL or other non NR running lines'), 
('VZ','Other passenger/external causes the responsibility of TOC'), 
('X1','Visibility in semaphore signalled areas, or special working for fog and falling snow implemented by Network Rail in all signalling areas'), 
('X2','Severe flooding beyond that which could be mitigated on Network Rail infrastructure'), 
('X3','Lightning Strike - damage to protected systems.'), 
('X4','Blanket speed restriction or Key Route Strategy implemented in accordance with Group Standards or other national operational safety instruction'), 
('X8','Animal Strike/Incursion not within the control of Network Rail'), 
('X9','Points failure caused by severe snow where heaters are working as designed'), 
('XA','Trespass'), 
('XB','Vandalism/theft (including the placing of objects on the line)'), 
('XC','Fatalities/injuries caused by being hit by train'), 
('XD','Level Crossing Incidents'), 
('XF','Police searching line'), 
('XH','Severe heat affecting infrastructure the responsibility of Network Rail (excluding Heat speeds)'), 
('XI','Security alert affecting Network Rail Network'), 
('XK','National Grid power failure - local area is also affected (includes ourages, surges and blips where a standby generator or UPS is installed and working)'), 
('XL','Fire external to railway infrastructure'), 
('XM','External utility incident including gas, water mains and overhead power lines'), 
('XN','Road or crossing related incidents including cars on the line, crossing misuse and priority given to emergency services over trains (excluding bridge strikes)'), 
('XO','External trees, building or objects encroaching onto Network Rail infrastructure (not due to weather or vandalism)'), 
('XP','Bridge Strike'), 
('XQ','Swing bridge open for river or canal traffic'), 
('XR','Cable vandalism/theft'), 
('XS','Level Crossing misuse'), 
('XT','Severe cold weather affecting infrastructure the responsibility of Network Rail'), 
('XU','Sunlight on signal'), 
('XV','Fire or evacuation due to fire alarm of Network Rail buildings other than stations due to vandalism'), 
('XW','High winds affecting infrastructure the responsibility of Network Rail including objects on the line due to the effect of weather'), 
('XZ','Other external causes the responsibility of Network Rail'), 
('YA','Lost path - regulated for train running on time'), 
('YB','Lost path - regulated for another late running train'), 
('YC','Lost path - following train running on time'), 
('YD','Lost path - following another late running train'), 
('YE','Lost path - waiting acceptance to single line'), 
('YF','Waiting for late running train off single line'), 
('YG','Regulated in accordance with Regulation Policy'), 
('YH','Late arrival of inward loco'), 
('YI','Late arrival of inward stock/unit'), 
('YJ','Late arrival of Traincrew on inward working'), 
('YK','Waiting connecting Freight or Res traffic to attach'), 
('YL','Waiting passenger connections within Connection Policy'), 
('YM','Special stop orders agreed by Control'), 
('YN','Booked traincrew not available for late running train'), 
('YO','Waiting platform/station congestion/platform change'), 
('YP','Delayed by diverted train'), 
('YQ','Passenger overcrowding caused by a train being of short- formation'), 
('YU','Service recovery - Booked rolling stock not available'),
('YV','Tactical hold of a train at origin or at a strategic location en-route due to a line blocking incident on the train\'s booked route'),
('YX','Passenger overcrowding caused by delay/cancellation of another train'), 
('ZW','Unattributed Cancellations'), 
('ZX','Unexplained late start'), 
('ZY','Unexplained Station overtime'), 
('ZZ','Unexplained loss in running');
