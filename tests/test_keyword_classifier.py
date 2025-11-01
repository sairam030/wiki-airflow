"""
Test keyword-based classifier with real Wikipedia data
"""
import sys
sys.path.insert(0, '/home/ram/wiki-trends-pipeline')

from plugins.operators.keyword_classifier import classify_with_keywords, extract_geography_with_keywords

# Your exact test data
test_pages = [
    {
        "title": "Trey_Yesavage",
        "categories": "2003 births|21st-century American sportsmen|All-American college baseball players|All Wikipedia articles written in American English|American expatriate baseball players in Canada|Articles with short description|Baseball players from Berks County, Pennsylvania|Buffalo Bisons (minor league) players|Charlottesville Tom Sox players|Dunedin Blue Jays players|East Carolina Pirates baseball players|Living people|Major League Baseball pitchers|New Hampshire Fisher Cats players|People from Boyertown, Pennsylvania|Short description is different from Wikidata|Short description matches Wikidata|Toronto Blue Jays players|Use American English from October 2025|Use mdy dates from October 2025|Vancouver Canadians players|Wikipedia pages semi-protected against vandalism",
        "expected": "Sports",
        "expected_location": "United States"
    },
    {
        "title": "Google_Chrome",
        "categories": "2008 software|All Wikipedia articles in need of updating|All Wikipedia articles written in American English|All articles containing potentially dated statements|All articles with unsourced statements|Android web browsers|Articles containing potentially dated statements from 2013|Articles containing potentially dated statements from April 2016|Articles containing potentially dated statements from February 2014|Articles containing potentially dated statements from July 2012|Articles containing potentially dated statements from May 2011|Articles containing potentially dated statements from September 2022|Articles containing potentially dated statements from September 2025|Articles with short description|Articles with unsourced statements from June 2023|C++ software|CS1: unfit URL|Cloud clients|Commons category link from Wikidata|Companies' terms of service|Cross-platform web browsers|Embedded Linux|Freeware|Google|Google Chrome|Google software|IOS web browsers|Linux web browsers|MacOS web browsers|Pages using bar box without float left or float right|Portable software|Proprietary freeware for Linux|Short description matches Wikidata|Site-specific browsing|Software based on WebKit|Software that uses FFmpeg|Use American English from February 2022|Use mdy dates from August 2022|Web browsers|Web browsers that use GTK|Webarchive template wayback links|Wikipedia articles in need of updating from December 2023|Wikipedia articles in need of updating from June 2019|Wikipedia articles in need of updating from November 2024|Wikipedia pages semi-protected against vandalism|Windows web browsers",
        "expected": "Technology",
        "expected_location": None
    },
    {
        "title": "Women's_Cricket_World_Cup",
        "categories": "All Wikipedia articles written in British English|Articles with short description|Commons category link is on Wikidata|International Cricket Council events|Recurring sporting events established in 1973|Short description is different from Wikidata|Use British English from March 2012|Use dmy dates from February 2021|Women's Cricket World Cup|Women's One Day International cricket competitions|Women's world championships|World championships in cricket|World cups",
        "expected": "Sports",
        "expected_location": None
    },
    {
        "title": "2025_Dutch_general_election",
        "categories": "2025 elections in Europe|2025 elections in the Caribbean|2025 elections in the Netherlands|2025 in Bonaire|2025 in Saba (island)|2025 in Sint Eustatius|Articles containing Dutch-language text|Articles with hCards|Articles with short description|CS1 Dutch-language sources (nl)|Elections in Bonaire|Elections in Saba (island)|Elections in Sint Eustatius|General elections in the Netherlands|Interlanguage link template existing link|October 2025 in the Netherlands|Short description is different from Wikidata|Use dmy dates from October 2025|Wikipedia pages semi-protected against vandalism",
        "expected": "Politics",
        "expected_location": None
    },
    {
        "title": "Prince_Andrew",
        "categories": "1960 births|All Wikipedia articles written in British English|All articles containing potentially dated statements|All articles lacking reliable references|All articles with dead external links|All articles with unsourced statements|Articles containing potentially dated statements from October 2025|Articles lacking reliable references from October 2025|Articles with dead external links from March 2023|Articles with hCards|Articles with permanently dead external links|Articles with short description|Articles with unsourced statements from May 2023|Biography with signature|British princes|CS1 Spanish-language sources (es)|Children of Elizabeth II|Commons category link from Wikidata|Dukes created by Elizabeth II|Earls of Inverness|English Anglicans|Falklands War pilots|Fleet Air Arm aviators|Graduates of Britannia Royal Naval College|Hereditary peers removed under the House of Lords Act 1999|Honorary air commodores|House of Windsor|Knights Grand Cross of the Royal Victorian Order|Knights of the Garter|Lakefield College School alumni|Living people|Lords High Commissioner to the General Assembly of the Church of Scotland|Mountbatten-Windsor family|Official website not in Wikidata|Pages containing London Gazette template with parameter supp set to y|Pages using infobox military person with embed|People appearing on C-SPAN|People associated with Jeffrey Epstein|People educated at Gordonstoun|People educated at Heatherdown School|Presidents of the Football Association|Prince Andrew|Recipients of the Order of Isabella the Catholic|Royal Navy personnel of the Falklands War|Royal Navy vice admirals|Short description is different from Wikidata|Sons of queens regnant|Use British English from October 2025|Use dmy dates from March 2025|Webarchive template wayback links|Wikipedia indefinitely semi-protected pages|Younger sons of dukes",
        "expected": "Politics",
        "expected_location": "United Kingdom"
    },
    {
        "title": "Jemimah_Rodrigues",
        "categories": "2000 births|All Wikipedia articles written in Indian English|Articles using Template:Medal with Runner-up|Articles using Template:Medal with Winner|Articles with short description|Asian Games gold medalists for India|Asian Games gold medalists in cricket|Commons category link from Wikidata|Commonwealth Games silver medallists for India|Commonwealth Games silver medallists in cricket|Cricketers at the 2022 Asian Games|Cricketers at the 2022 Commonwealth Games|Cricketers from Mumbai|Delhi Capitals (WPL) cricketers|IPL Supernovas cricketers|IPL Trailblazers cricketers|India women One Day International cricketers|India women Test cricketers|India women Twenty20 International cricketers|Indian Roman Catholics|Indian expatriate cricketers in Australia|Indian expatriate cricketers in England|Indian women cricketers|Living people|Mangalorean Catholics|Mangaloreans|Medalists at the 2022 Asian Games|Medallists at the 2022 Commonwealth Games|Melbourne Renegades (WBBL) cricketers|Melbourne Stars (WBBL) cricketers|Mumbai women cricketers|Northern Superchargers cricketers|Pages containing links to subscription-only content|Short description is different from Wikidata|Sportswomen from Maharashtra|Use Indian English from January 2019|Use dmy dates from January 2019|West Zone women cricketers|Yorkshire Diamonds cricketers",
        "expected": "Sports",
        "expected_location": "India"
    },
    {
        "title": "XXX_(2002_film)",
        "categories": "2000s American films|2000s Czech-language films|2000s English-language films|2000s German-language films|2000s Russian-language films|2000s Spanish-language films|2000s spy films|2002 action thriller films|2002 films|All Wikipedia articles written in American English|All articles with dead external links|All articles with unsourced statements|American action adventure films|American action thriller films|American spy action films|American spy films|American techno-thriller films|Articles with dead external links from August 2020|Articles with permanently dead external links|Articles with short description|Articles with unsourced statements from June 2024|Articles with unsourced statements from October 2017|Avalanches in film|CS1 maint: numeric names: authors list|Columbia Pictures films|English-language action adventure films|English-language action thriller films|Films directed by Rob Cohen|Films produced by Neal H. Moritz|Films scored by Randy Edelman|Films set in California|Films set in Colombia|Films set in Prague|Films shot in Austria|Films shot in Bora Bora|Films shot in California|Films shot in Los Angeles|Films shot in West Virginia|Films shot in the Czech Republic|Original Film films|Revolution Studios films|Short description is different from Wikidata|Template film date with 1 release date|Use American English from January 2025|Wikipedia pages semi-protected against vandalism|XXX (film series)",
        "expected": "Entertainment",
        "expected_location": "United States"
    },
    {
        "title": "6-7_(meme)",
        "categories": "2020s slang|All Wikipedia articles written in American English|Articles with redirect hatnotes impacted by RfD|Articles with short description|Generation Alpha slang|Generation Z slang|Internet memes introduced in 2025|Internet slang|Numerical memes|Short description is different from Wikidata|TikTok trends|Use American English from October 2025|Use mdy dates from October 2025|Wikipedia semi-protected pages",
        "expected": "Other",  # This one is tricky - could be Entertainment
        "expected_location": "United States"
    },
]

print("="*100)
print("KEYWORD-BASED CLASSIFICATION TEST")
print("="*100)

correct = 0
total = len(test_pages)

for page in test_pages:
    title = page["title"]
    categories = page["categories"]
    expected = page["expected"]
    
    # Test classification
    result = classify_with_keywords(title, categories)
    is_correct = result == expected
    
    if is_correct:
        correct += 1
        status = "✅"
    else:
        status = "❌"
    
    print(f"\n{status} {title.replace('_', ' ')}")
    print(f"   Expected: {expected}")
    print(f"   Got:      {result}")
    
    # Test geography
    geo = extract_geography_with_keywords(title, categories)
    if geo:
        print(f"   Location: {geo['location_type']} - {geo['location']}")
    else:
        print(f"   Location: None")

print(f"\n{'='*100}")
print(f"ACCURACY: {correct}/{total} = {(correct/total)*100:.1f}%")
print(f"{'='*100}")

print("\n🚀 SPEED TEST: Processing 100 pages...")
import time
start = time.time()

for i in range(100):
    for page in test_pages:
        classify_with_keywords(page["title"], page["categories"])
        extract_geography_with_keywords(page["title"], page["categories"])

elapsed = time.time() - start
per_page = (elapsed / (100 * len(test_pages))) * 1000

print(f"✅ Processed {100 * len(test_pages)} classifications in {elapsed:.2f}s")
print(f"⚡ Speed: {per_page:.2f}ms per page ({1000/per_page:.0f} pages/second)")
print(f"📊 Estimated time for 995 pages: {(995 * per_page / 1000):.1f}s = ~{(995 * per_page / 1000 / 60):.1f} minutes")
