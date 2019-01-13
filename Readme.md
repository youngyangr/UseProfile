User Profile Project
===========
weibo basic, interest, network, profession

1. weibo basic includes uid, nickname, region, sex, age, description

2. interest use TLDA algorithm to obtain interest and judge whether number of weibo is more than 5

3. network use graphX calculation and accomplish community by map and reduce, use pagerank algorithm

4. profession include four professions, they are teacher, student, commerce, doctor, match profession by match key words.
 

Interest detect use IKAnalyzer as a chinese word segmentation.

Final result is mysql tables which include each area's information.

In this project, origin data is 1.2G, and use whole 1.2G data to obtain result.

