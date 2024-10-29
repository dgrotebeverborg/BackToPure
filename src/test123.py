import time
import pandas as pd
import logging
from logging_config import setup_logging
import requests
import json
import argparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
from config import PURE_BASE_URL, PURE_API_KEY, PURE_HEADERS, RIC_BASE_URL, OPENALEXEX_ID_URI, ORCID_ID_URI, OPENALEX_HEADERS
logger = setup_logging('external persons', level=logging.INFO)

#
# headers = {
#     'Accept': 'application/json',
#     'api-key': PURE_API_KEY,
# }
#
# jsondata = {'size': 100, 'searchString': '10.1017/s1053837222000402|10.1111/puar.13583|10.1016/j.econlet.2023.111256|10.1038/d41586-023-02698-z|10.1016/j.esg.2023.100199|10.1016/j.socscimed.2023.116169|10.3390/su151310281|10.1093/jiel/jgad015|10.1016/j.shpsa.2023.08.007|10.1002/vro2.67|10.1163/15718085-bja10132|10.1007/s12142-023-00691-7|10.1038/s41598-023-42204-z|10.1016/j.tate.2022.103977|10.1007/s12152-023-09526-1|10.1017/glj.2023.80|10.1080/2474736x.2023.2195476|10.3390/soc13080196|10.1080/1460728x.2023.2235182|10.1080/17441056.2023.2200623|10.1016/j.ufug.2023.127999'}
# # Replace '|' with ',' to make the DOIs comma-separated
# jsondata['searchString'] = jsondata['searchString'].replace('|', ',')
# session = requests.Session()
# # Make the API request using the pre-configured session
# response = session.post(
#     PURE_BASE_URL + 'research-outputs/search',
#     headers=headers,  # Use the headers defined earlier
#     json=jsondata,
#     timeout= 100  # Set a timeout for the request
# )
# # Parse the response JSON
# data = response.json()
# returned_items = data.get('count')
# # Count the DOIs
# doi_count = len(jsondata['searchString'].split('|'))
# print(f"Total items asked: {str(doi_count)}")
# print(f"Total items found: {data.get('count', 0)}")
#
# works = data.get("items", [])  # Extract the list of items
# # Create a set of returned DOIs from the API response, normalized to lowercase
# returned_dois = set()
# for item in works:
#     for version in item.get("electronicVersions", []):
#         if "doi" in version:
#             returned_dois.add(version["doi"].lower())  # Normalize to lowercase
#
# print('asked dois: ', jsondata['searchString'])
#
# print('returned dois: ', returned_dois)

ask = ['10.4467/25444654spp.23.006.17608', '10.1007/s11625-023-01424-y', '10.5553/rem/.000072', '10.5117/thrm2023.2.007.helf', '10.3389/fpsyg.2023.1025153', '10.1016/j.econlet.2023.111256', '10.1093/biosci/biad096', '10.1016/j.jenvman.2023.117424', '10.1163/15718085-bja10132', '10.1186/s13643-023-02257-7', '10.1016/j.jbef.2023.100819', '10.1016/j.respol.2022.104712', '10.2139/ssrn.4525843', '10.1080/1460728x.2023.2235182', '10.1016/j.ssaho.2023.100416', '10.5553/mvv/157457672023033001006', '10.5553/bsb/266669012023004001007', '10.5553/elr.000231', '10.1016/j.respol.2023.104840', '10.1111/1748-8583.12524', '10.5553/bsb/266669012023004005007', '10.1093/restud/rdad026', '10.1016/j.jebo.2023.03.010', '10.1017/glj.2023.24', '10.1016/j.jfi.2022.101016', '10.35295/osls.iisl.1672', '10.3390/smartcities6020042', '10.1111/joes.12558', '10.36633/ulr.879', '10.1016/bs.aecr.2023.10.005', '10.1016/j.envsci.2022.10.011', '10.3390/su15020935', '10.3233/ip-239009', '10.1016/j.labeco.2023.102411', '10.1016/j.strueco.2023.02.006', '10.1016/j.esg.2023.100199', '10.1371/journal.pone.0284390', '10.5117/tva2023.3.002.bekk', '10.1093/jlb/lsad009', '10.1002/ejsp.2948', '10.1016/j.oneear.2023.11.009', '10.1080/01639625.2023.2225678', '10.2139/ssrn.4425414', '10.1016/j.jimonfin.2022.102798', '10.3390/su15065546', '10.1111/radm.12571', '10.5195/rt.2023.1133', '10.1080/03069400.2023.2275495', '10.2139/ssrn.4403814', '10.1177/0734371x231198163', '10.1017/s1053837222000700', '10.1080/1460728x.2023.2235178', '10.1016/j.socscimed.2023.116169', '10.3390/children10050872', '10.1080/14719037.2023.2222734', '10.1017/s1053837222000402', '10.1177/00197939231173676', '10.3390/soc13080196', '10.1017/sus.2023.8', '10.1080/25741292.2023.2272332', '10.1093/jiel/jgad015', '10.1016/j.ecolecon.2023.107797', '10.1111/puar.13583', '10.1016/j.jbusres.2022.113322', '10.1371/journal.pone.0289705', '10.1016/j.ufug.2023.127999', '10.1017/mem.2023.6', '10.1016/j.hrmr.2022.100952', '10.1080/13229400.2023.2181849', '10.1016/j.eneco.2023.106794', '10.25112/bcij.v3i1.3328', '10.1111/puar.13602', '10.1007/s11229-023-04120-7', '10.5117/kwa2023.1.011.stev', '10.1093/jicj/mqad025', '10.1177/20322844231153829', '10.1017/s2047102523000043', '10.1038/d41586-023-02698-z', '10.1215/00182702-10621040', '10.1016/j.ijdrr.2023.103840', '10.5553/mvo/245231352023009005005', '10.1073/pnas.2215465120', '10.36633/ulr.940', '10.1080/14719037.2023.2254306', '10.1093/qopen/qoad014', '10.1287/mnsc.2023.4685', '10.1002/sd.2595', '10.1111/joes.12582', '10.1038/s41597-023-02569-2', '10.3389/fdgth.2023.1149374', '10.1093/ppmgov/gvac025', '10.1177/17427150231198978', '10.1561/0300000089', '10.3389/fcomp.2023.1149305', '10.1080/21606544.2023.2166129', '10.1093/ejil/chad015', '10.1177/02750740231155408', '10.1093/idpl/ipad019', '10.1016/j.jcorpfin.2022.102321', '10.17645/pag.v11i3.7163', '10.1163/15718034-bja10096', '10.1016/j.respol.2023.104822', '10.1016/j.reseneeco.2023.101387', '10.1093/grurint/ikac143', '10.1177/13882627231170856', '10.1097/jfn.0000000000000424', '10.5194/nhess-23-179-2023', '10.1016/j.jcorpfin.2022.102322', '10.1080/17441056.2023.2200623', '10.1163/26663236-bja10061', '10.1016/j.scaman.2023.101260', '10.36633/ulr.862', '10.5553/rem/.000074', '10.1007/s11625-023-01449-3', '10.1163/15718085-bja10131', '10.1111/puar.13584', '10.1029/2022wr034192', '10.2139/ssrn.4337524', '10.1163/15718190-2023xx07', '10.3389/fpubh.2023.1209965', '10.1017/glj.2023.80', '10.1111/twec.13503', '10.1002/vro2.67', '10.1002/gsj.1496', '10.1016/j.jbef.2023.100834', '10.1038/s41539-023-00189-4', '10.5553/tvgr/016508742023047003002', '10.36633/ulr.913', '10.1016/j.tra.2023.103831', '10.1007/s12142-023-00691-7', '10.1108/lhs-05-2023-0034', '10.1016/j.tate.2022.103977', '10.1177/20322844231212661', '10.3390/su151310281', '10.1017/glj.2023.52', '10.1016/j.ijme.2023.100896', '10.1038/s41598-023-42204-z', '10.1080/2474736x.2023.2195476', '10.1007/s12152-023-09526-1', '10.1111/twec.13392', '10.1080/1460728x.2023.2235183', '10.36633/ulr.817', '10.1016/j.shpsa.2023.08.007', '10.1080/14719037.2023.2222729', '10.1177/10422587221145676', '10.36633/ulr.877', '10.1017/bpp.2023.18', '10.1016/j.pacfin.2023.102010', '10.1002/bsl.2618', '10.1016/j.clsr.2023.105870', '10.1016/j.ssaho.2023.100660', '10.1111/puar.13598', '10.1016/j.gloenvcha.2023.102736']

batch5_1 =  {'10.4467/25444654spp.23.006.17608', '10.1093/jpepsy/jsac041', '10.1007/s11625-023-01424-y', '10.5553/rem/.000072', '10.5117/thrm2023.2.007.helf', '10.3389/fpsyg.2023.1025153', '10.1016/j.econlet.2023.111256', '10.1093/biosci/biad096', '10.1016/j.jenvman.2023.117424', '10.1163/15718085-bja10132', '10.1186/s13643-023-02257-7', '10.1016/j.jbef.2023.100819', '10.2139/ssrn.4525843', '10.1016/j.respol.2022.104712', '10.2136/vzj2018.09.0167', '10.1111/jora.12539', '10.1080/1460728x.2023.2235182', '10.1016/j.ssaho.2023.100416', '10.5553/mvv/157457672023033001006', '10.1029/2021ef002576', '10.5553/bsb/266669012023004001007', '10.5553/elr.000231', '10.1016/j.respol.2023.104840', '10.1111/1748-8583.12524', '10.5553/bsb/266669012023004005007', '10.1093/restud/rdad026', '10.1016/j.jebo.2023.03.010', '10.1017/glj.2023.24', '10.1016/j.jfi.2022.101016', '10.35295/osls.iisl.1672', '10.3390/smartcities6020042', '10.1111/joes.12558', '10.36633/ulr.879', '10.1016/bs.aecr.2023.10.005', '10.1016/j.envsci.2022.10.011', '10.3390/su15020935', '10.3233/ip-239009', '10.1177/0022219417708172', '10.1016/j.labeco.2023.102411', '10.1016/j.strueco.2023.02.006', '10.1016/j.esg.2023.100199', '10.1371/journal.pone.0284390', '10.1002/ejsp.2948', '10.5117/tva2023.3.002.bekk', '10.1093/jlb/lsad009', '10.1016/j.oneear.2023.11.009', '10.33540/2080', '10.3389/fped.2022.828448', '10.1007/s10803-017-3071-y', '10.2139/ssrn.4425414', '10.1016/j.jimonfin.2022.102798', '10.3390/su15065546', '10.5194/essd-14-4365-2022', '10.1111/radm.12571', '10.5195/rt.2023.1133', '10.1080/03069400.2023.2275495', '10.1016/j.nicl.2022.103057', '10.2139/ssrn.4403814', '10.1177/0734371x231198163', '10.1017/s1053837222000700', '10.1080/1460728x.2023.2235178', '10.1016/j.socscimed.2023.116169', '10.3390/children10050872', '10.1080/14719037.2023.2222734', '10.1017/s1053837222000402', '10.1177/00197939231173676', '10.3390/soc13080196', '10.1017/sus.2023.8', '10.1080/25741292.2023.2272332', '10.1016/j.ijpharm.2017.03.036', '10.1093/jiel/jgad015', '10.1016/j.ecolecon.2023.107797', '10.1111/puar.13583', '10.1016/j.jbusres.2022.113322', '10.1371/journal.pone.0289705', '10.1016/j.ufug.2023.127999', '10.1017/mem.2023.6', '10.1029/2022jb024191', '10.1016/j.hrmr.2022.100952', '10.1080/13229400.2023.2181849', '10.1016/j.eneco.2023.106794', '10.25112/bcij.v3i1.3328', '10.1111/puar.13602', '10.3168/jds.2016-11575', '10.1007/s11229-023-04120-7', '10.5117/kwa2023.1.011.stev', '10.1093/jicj/mqad025', '10.33540/1840', '10.1177/20322844231153829', '10.1017/s2047102523000043', '10.36633/ulr.798', '10.1038/d41586-023-02698-z', '10.1111/risa.13905', '10.1215/00182702-10621040', '10.1016/j.ijdrr.2023.103840', '10.5553/mvo/245231352023009005005', '10.1073/pnas.2215465120', '10.36633/ulr.940', '10.1080/14719037.2023.2254306', '10.1093/qopen/qoad014', '10.1287/mnsc.2023.4685', '10.1002/sd.2595', '10.1038/s41597-023-02569-2', '10.1111/joes.12582', '10.3389/fdgth.2023.1149374', '10.1561/0300000089', '10.1093/ppmgov/gvac025', '10.1177/17427150231198978', '10.3389/fcomp.2023.1149305', '10.1080/21606544.2023.2166129', '10.3390/app13063747', '10.7554/elife.73827', '10.1093/ejil/chad015', '10.5194/nhess-23-65-2023', '10.1177/02750740231155408', '10.1093/idpl/ipad019', '10.1016/j.jcorpfin.2022.102321', '10.17645/pag.v11i3.7163', '10.1163/15718034-bja10096', '10.1016/j.respol.2023.104822', '10.1016/j.reseneeco.2023.101387', '10.1093/grurint/ikac143', '10.1177/13882627231170856', '10.5194/nhess-23-179-2023', '10.1016/j.envsci.2023.103598', '10.1097/jfn.0000000000000424', '10.1016/j.jcorpfin.2022.102322', '10.33540/1755', '10.1080/17441056.2023.2200623', '10.1163/26663236-bja10061', '10.1016/j.scaman.2023.101260', '10.36633/ulr.862', '10.5553/rem/.000074', '10.1007/s11625-023-01449-3', '10.1016/j.ssaho.2023.100660', '10.1163/15718085-bja10131', '10.1111/puar.13584', '10.1029/2022wr034192', '10.5194/gmd-10-1261-2017', '10.2139/ssrn.4337524', '10.1163/15718190-2023xx07', '10.3389/fpubh.2023.1209965', '10.1017/glj.2023.80', '10.1111/twec.13503', '10.1002/vro2.67', '10.1002/gsj.1496', '10.1016/j.jbef.2023.100834', '10.1038/s41539-023-00189-4', '10.5553/tvgr/016508742023047003002', '10.36633/ulr.913', '10.1016/j.tra.2023.103831', '10.1007/s12142-023-00691-7', '10.1108/lhs-05-2023-0034', '10.1016/j.tate.2022.103977', '10.1177/20322844231212661', '10.17239/jowr-2018.09.03.02', '10.3390/su151310281', '10.3168/jds.2015-9837', '10.1007/978-3-031-15904-6', '10.1017/glj.2023.52', '10.1016/j.gloenvcha.2023.102672', '10.1038/s41598-023-42204-z', '10.1080/2474736x.2023.2195476', '10.1007/s12152-023-09526-1', '10.1111/twec.13392', '10.1080/1460728x.2023.2235183', '10.36633/ulr.817', '10.1016/j.shpsa.2023.08.007', '10.1177/10422587221145676', '10.1080/14719037.2023.2222729', '10.36633/ulr.877', '10.1136/oemed-2018-105108', '10.1017/bpp.2023.18', '10.1037/hea0001099', '10.1016/j.pacfin.2023.102010', '10.1002/bsl.2618', '10.1016/j.clsr.2023.105870', '10.1080/01639625.2023.2225678', '10.1111/puar.13598', '10.1016/j.gloenvcha.2023.102736'}
batch5_2 =  {'10.1016/j.clsr.2023.105870', '10.1186/s13643-023-02257-7', '10.1016/j.eneco.2023.106794', '10.5553/mvv/157457672023033001006', '10.1093/jpepsy/jsac041', '10.1017/s1053837222000700', '10.1177/17427150231198978', '10.1080/01639625.2023.2225678', '10.5195/rt.2023.1133', '10.1002/ejsp.2948', '10.1007/s11625-023-01449-3', '10.1111/joes.12558', '10.1007/s12142-023-00691-7', '10.1017/glj.2023.52', '10.36633/ulr.879', '10.3390/soc13080196', '10.1038/s41598-023-42204-z', '10.1080/1460728x.2023.2235182', '10.1080/1460728x.2023.2235183', '10.1016/j.ecolecon.2023.107797', '10.1016/j.pacfin.2023.102010', '10.1016/j.ijdrr.2023.103840', '10.1007/s11625-023-01424-y', '10.36633/ulr.798', '10.36633/ulr.877', '10.5117/thrm2023.2.007.helf', '10.1017/glj.2023.80', '10.1177/02750740231155408', '10.1080/2474736x.2023.2195476', '10.3389/fdgth.2023.1149374', '10.5553/bsb/266669012023004001007', '10.1016/j.jbef.2023.100834', '10.1038/s41539-023-00189-4', '10.1080/03069400.2023.2275495', '10.1371/journal.pone.0284390', '10.1016/j.respol.2023.104822', '10.5553/elr.000231', '10.25112/bcij.v3i1.3328', '10.5194/nhess-23-65-2023', '10.1177/0734371x231198163', '10.3389/fcomp.2023.1149305', '10.1017/glj.2023.24', '10.1016/j.jimonfin.2022.102798', '10.1002/bsl.2618', '10.1017/mem.2023.6', '10.1108/lhs-05-2023-0034', '10.1287/mnsc.2023.4685', '10.1080/14719037.2023.2222729', '10.1093/ejil/chad015', '10.1038/s41597-023-02569-2', '10.5117/tva2023.3.002.bekk', '10.1016/j.socscimed.2023.116169', '10.36633/ulr.862', '10.1016/j.hrmr.2022.100952', '10.1080/25741292.2023.2272332', '10.1016/j.jenvman.2023.117424', '10.1093/biosci/biad096', '10.1016/j.strueco.2023.02.006', '10.1016/j.ufug.2023.127999', '10.1073/pnas.2215465120', '10.1016/j.scaman.2023.101260', '10.1037/hea0001099', '10.1016/j.ssaho.2023.100660', '10.1177/13882627231170856', '10.1016/j.respol.2022.104712', '10.1111/jora.12539', '10.3389/fped.2022.828448', '10.1136/oemed-2018-105108', '10.1111/puar.13584', '10.1177/20322844231212661', '10.1016/j.ssaho.2023.100416', '10.3390/su15020935', '10.33540/1840', '10.1016/j.envsci.2023.103598', '10.1016/j.econlet.2023.111256', '10.1111/joes.12582', '10.1177/20322844231153829', '10.1016/j.labeco.2023.102411', '10.1111/1748-8583.12524', '10.1080/17441056.2023.2200623', '10.5553/rem/.000072', '10.1080/14719037.2023.2254306', '10.1111/risa.13905', '10.1177/0022219417708172', '10.1016/j.ijpharm.2017.03.036', '10.1016/j.envsci.2022.10.011', '10.3390/su15065546', '10.2139/ssrn.4425414', '10.3390/su151310281', '10.1080/21606544.2023.2166129', '10.1007/s12152-023-09526-1', '10.1163/15718085-bja10131', '10.1163/15718190-2023xx07', '10.1093/qopen/qoad014', '10.1093/jicj/mqad025', '10.5194/essd-14-4365-2022', '10.1029/2022wr034192', '10.5553/mvo/245231352023009005005', '10.1016/j.jbef.2023.100819', '10.1163/15718085-bja10132', '10.1007/s10803-017-3071-y', '10.1093/jiel/jgad015', '10.1029/2021ef002576', '10.1111/puar.13602', '10.3168/jds.2015-9837', '10.35295/osls.iisl.1672', '10.1016/j.jbusres.2022.113322', '10.5194/nhess-23-179-2023', '10.3390/smartcities6020042', '10.36633/ulr.913', '10.1016/j.respol.2023.104840', '10.1163/15718034-bja10096', '10.1016/j.oneear.2023.11.009', '10.1017/sus.2023.8', '10.1016/j.jcorpfin.2022.102322', '10.1007/978-3-031-15904-6', '10.17645/pag.v11i3.7163', '10.1017/s2047102523000043', '10.1111/puar.13583', '10.2136/vzj2018.09.0167', '10.1002/sd.2595', '10.36633/ulr.817', '10.1177/00197939231173676', '10.7554/elife.73827', '10.2139/ssrn.4337524', '10.1038/d41586-023-02698-z', '10.1007/s11229-023-04120-7', '10.17239/jowr-2018.09.03.02', '10.1016/bs.aecr.2023.10.005', '10.1561/0300000089', '10.1111/radm.12571', '10.1080/14719037.2023.2222734', '10.1093/idpl/ipad019', '10.1215/00182702-10621040', '10.3390/app13063747', '10.1093/grurint/ikac143', '10.2139/ssrn.4403814', '10.1080/1460728x.2023.2235178', '10.1016/j.gloenvcha.2023.102672', '10.1371/journal.pone.0289705', '10.33540/1755', '10.4467/25444654spp.23.006.17608', '10.3389/fpubh.2023.1209965', '10.1016/j.jebo.2023.03.010', '10.5194/gmd-10-1261-2017', '10.1016/j.esg.2023.100199', '10.36633/ulr.940', '10.2139/ssrn.4525843', '10.1002/vro2.67', '10.3389/fpsyg.2023.1025153', '10.1093/ppmgov/gvac025', '10.1016/j.shpsa.2023.08.007', '10.1111/twec.13392', '10.5117/kwa2023.1.011.stev', '10.1093/restud/rdad026', '10.1163/26663236-bja10061', '10.3233/ip-239009', '10.1016/j.tate.2022.103977', '10.1111/puar.13598', '10.33540/2080', '10.5553/tvgr/016508742023047003002', '10.1097/jfn.0000000000000424', '10.5553/bsb/266669012023004005007', '10.1002/gsj.1496', '10.1016/j.gloenvcha.2023.102736', '10.1080/13229400.2023.2181849', '10.1017/bpp.2023.18', '10.1016/j.jfi.2022.101016', '10.5553/rem/.000074', '10.3168/jds.2016-11575', '10.1016/j.tra.2023.103831', '10.1016/j.nicl.2022.103057', '10.1016/j.reseneeco.2023.101387', '10.1111/twec.13503', '10.3390/children10050872', '10.1017/s1053837222000402', '10.1029/2022jb024191', '10.1093/jlb/lsad009', '10.1016/j.jcorpfin.2022.102321', '10.1177/10422587221145676'}
batch5_3 =  {'10.1080/1460728x.2023.2235178', '10.1017/glj.2023.24', '10.1002/bsl.2618', '10.1186/s13643-023-02257-7', '10.5553/bsb/266669012023004005007', '10.1002/gsj.1496', '10.1016/j.respol.2023.104840', '10.1016/j.labeco.2023.102411', '10.1016/j.pacfin.2023.102010', '10.1080/14719037.2023.2254306', '10.1016/j.hrmr.2022.100952', '10.5194/gmd-10-1261-2017', '10.1016/j.eneco.2023.106794', '10.3390/app13063747', '10.1016/j.econlet.2023.111256', '10.1080/2474736x.2023.2195476', '10.1037/hea0001099', '10.1093/idpl/ipad019', '10.1017/s2047102523000043', '10.33540/1755', '10.36633/ulr.940', '10.3389/fped.2022.828448', '10.36633/ulr.877', '10.5194/essd-14-4365-2022', '10.1136/oemed-2018-105108', '10.3389/fdgth.2023.1149374', '10.2139/ssrn.4403814', '10.17645/pag.v11i3.7163', '10.3390/su15020935', '10.1215/00182702-10621040', '10.1016/j.nicl.2022.103057', '10.5117/thrm2023.2.007.helf', '10.4467/25444654spp.23.006.17608', '10.3390/soc13080196', '10.17239/jowr-2018.09.03.02', '10.1017/s1053837222000402', '10.36633/ulr.913', '10.1093/ppmgov/gvac025', '10.1038/s41539-023-00189-4', '10.3168/jds.2015-9837', '10.1016/j.respol.2023.104822', '10.1016/j.jcorpfin.2022.102321', '10.1177/20322844231212661', '10.5553/mvv/157457672023033001006', '10.3168/jds.2016-11575', '10.1093/jlb/lsad009', '10.1080/21606544.2023.2166129', '10.36633/ulr.798', '10.3233/ip-239009', '10.1029/2022jb024191', '10.1561/0300000089', '10.1016/j.gloenvcha.2023.102736', '10.1093/ejil/chad015', '10.1016/j.ufug.2023.127999', '10.3389/fpsyg.2023.1025153', '10.1111/puar.13598', '10.1093/jpepsy/jsac041', '10.1016/j.esg.2023.100199', '10.1016/j.jfi.2022.101016', '10.33540/1840', '10.1111/twec.13503', '10.1016/j.reseneeco.2023.101387', '10.1093/jicj/mqad025', '10.1163/26663236-bja10061', '10.5553/tvgr/016508742023047003002', '10.1111/jora.12539', '10.1007/978-3-031-15904-6', '10.1097/jfn.0000000000000424', '10.1029/2022wr034192', '10.1080/01639625.2023.2225678', '10.5553/elr.000231', '10.1111/twec.13392', '10.3389/fcomp.2023.1149305', '10.2139/ssrn.4425414', '10.2136/vzj2018.09.0167', '10.1016/j.jcorpfin.2022.102322', '10.1016/j.jimonfin.2022.102798', '10.1016/j.envsci.2023.103598', '10.1038/s41598-023-42204-z', '10.1017/sus.2023.8', '10.1111/puar.13583', '10.1177/10422587221145676', '10.1038/d41586-023-02698-z', '10.5553/bsb/266669012023004001007', '10.1016/j.ijdrr.2023.103840', '10.1080/17441056.2023.2200623', '10.1080/13229400.2023.2181849', '10.1016/j.strueco.2023.02.006', '10.3389/fpubh.2023.1209965', '10.35295/osls.iisl.1672', '10.1111/joes.12558', '10.1017/s1053837222000700', '10.1007/s12152-023-09526-1', '10.1007/s12142-023-00691-7', '10.1177/02750740231155408', '10.3390/su151310281', '10.1177/13882627231170856', '10.1080/03069400.2023.2275495', '10.2139/ssrn.4525843', '10.3390/smartcities6020042', '10.1371/journal.pone.0284390', '10.5117/tva2023.3.002.bekk', '10.7554/elife.73827', '10.1073/pnas.2215465120', '10.3390/su15065546', '10.36633/ulr.817', '10.1111/joes.12582', '10.1017/glj.2023.52', '10.1002/ejsp.2948', '10.2139/ssrn.4337524', '10.36633/ulr.862', '10.1016/j.tate.2022.103977', '10.1007/s10803-017-3071-y', '10.5117/kwa2023.1.011.stev', '10.36633/ulr.879', '10.5195/rt.2023.1133', '10.1002/sd.2595', '10.1093/jiel/jgad015', '10.1016/j.jenvman.2023.117424', '10.1177/0022219417708172', '10.1016/j.jbusres.2022.113322', '10.1016/j.shpsa.2023.08.007', '10.1177/0734371x231198163', '10.1111/risa.13905', '10.1016/j.gloenvcha.2023.102672', '10.1080/25741292.2023.2272332', '10.1177/00197939231173676', '10.1163/15718085-bja10132', '10.1080/1460728x.2023.2235182', '10.1016/j.ijpharm.2017.03.036', '10.1108/lhs-05-2023-0034', '10.3390/children10050872', '10.1017/bpp.2023.18', '10.1371/journal.pone.0289705', '10.1080/1460728x.2023.2235183', '10.1093/grurint/ikac143', '10.1177/20322844231153829', '10.25112/bcij.v3i1.3328', '10.33540/2080', '10.5553/rem/.000074', '10.1016/j.ssaho.2023.100416', '10.5553/rem/.000072', '10.1177/17427150231198978', '10.1007/s11625-023-01424-y', '10.1007/s11625-023-01449-3', '10.1007/s11229-023-04120-7', '10.1016/j.respol.2022.104712', '10.1002/vro2.67', '10.1016/j.ecolecon.2023.107797', '10.1163/15718085-bja10131', '10.1111/radm.12571', '10.1016/j.ssaho.2023.100660', '10.1016/j.jbef.2023.100834', '10.1111/puar.13584', '10.1016/j.tra.2023.103831', '10.1016/j.jebo.2023.03.010', '10.5553/mvo/245231352023009005005', '10.1111/puar.13602', '10.1287/mnsc.2023.4685', '10.1163/15718034-bja10096', '10.5194/nhess-23-179-2023', '10.1017/glj.2023.80', '10.1016/j.envsci.2022.10.011', '10.1093/qopen/qoad014', '10.1029/2021ef002576', '10.1016/j.socscimed.2023.116169', '10.1017/mem.2023.6', '10.1093/restud/rdad026', '10.1080/14719037.2023.2222734', '10.1038/s41597-023-02569-2', '10.1111/1748-8583.12524', '10.1016/bs.aecr.2023.10.005', '10.1016/j.scaman.2023.101260', '10.1016/j.jbef.2023.100819', '10.1080/14719037.2023.2222729', '10.1163/15718190-2023xx07', '10.1016/j.oneear.2023.11.009', '10.1016/j.clsr.2023.105870', '10.5194/nhess-23-65-2023', '10.1093/biosci/biad096'}

print(type(batch5_1))

ask_set = set(ask)

# Find DOIs in 'ask' and in each batch set separately
dois_in_ask_and_batch5_1 = ask_set.intersection(batch5_1)
dois_in_ask_and_batch5_2 = ask_set.intersection(batch5_2)
dois_in_ask_and_batch5_3 = ask_set.intersection(batch5_3)

# Output results for each batch
print(f"DOIs in 'ask' and in batch5_1: {len(dois_in_ask_and_batch5_1)}, DOIs: {dois_in_ask_and_batch5_1}")
print(f"DOIs in 'ask' and in batch5_2: {len(dois_in_ask_and_batch5_2)}, DOIs: {dois_in_ask_and_batch5_2}")
print(f"DOIs in 'ask' and in batch5_3: {len(dois_in_ask_and_batch5_3)}, DOIs: {dois_in_ask_and_batch5_3}")

# Find DOIs that are in 'ask' and in all three batch sets (i.e., present in each of them)
dois_in_all_batches = ask_set.intersection(batch5_1, batch5_2, batch5_3)

# Output result for DOIs in all three batches
print(f"DOIs in 'ask' and in all batches: {len(dois_in_all_batches)}, DOIs: {dois_in_all_batches}")

# Find DOIs in 'ask' but not in each batch set separately
dois_in_ask_not_in_batch5_1 = ask_set.difference(batch5_1)
dois_in_ask_not_in_batch5_2 = ask_set.difference(batch5_2)
dois_in_ask_not_in_batch5_3 = ask_set.difference(batch5_3)

# Output results for each batch
print(f"DOIs in 'ask' but not in batch5_1: {len(dois_in_ask_not_in_batch5_1)}, DOIs: {dois_in_ask_not_in_batch5_1}")
print(f"DOIs in 'ask' but not in batch5_2: {len(dois_in_ask_not_in_batch5_2)}, DOIs: {dois_in_ask_not_in_batch5_2}")
print(f"DOIs in 'ask' but not in batch5_3: {len(dois_in_ask_not_in_batch5_3)}, DOIs: {dois_in_ask_not_in_batch5_3}")

# Find DOIs that are in 'ask' but not in any of the batches
all_batches = batch5_1.union(batch5_2, batch5_3)
dois_in_ask_not_in_any_batch = ask_set.difference(all_batches)

# Output result for DOIs in 'ask' but not in any of the batches
print(f"DOIs in 'ask' but not in any of the batches: {len(dois_in_ask_not_in_any_batch)}, DOIs: {dois_in_ask_not_in_any_batch}")