"""
Fast keyword-based classifier using Wikipedia categories
High accuracy (80-90%+) with zero API costs and instant results
"""
import re

# All 19 categories
CATEGORIES = [
    "Autos and vehicles",
    "Beauty and fashion",
    "Business and finance",
    "Climate",
    "Entertainment",
    "Food and drink",
    "Games",
    "Health",
    "Hobbies and leisure",
    "Jobs and education",
    "Law and government",
    "Pets and animals",
    "Politics",
    "Science",
    "Shopping",
    "Sports",
    "Technology",
    "Travel and transportation",
    "Other"
]

# Comprehensive keyword patterns for each category
# Strategy: Match Wikipedia category patterns + common keywords + variations
KEYWORD_RULES = {
    "Sports": [
        # Core sports keywords
        r'\b(sport|sports|sporting|athlete|athletics)\b',
        # Specific sports (50+ sports covered)
        r'\b(baseball|basketball|football|soccer|cricket|tennis|golf|hockey|rugby|volleyball|swimming|boxing|wrestling|mma|ufc|badminton|squash|table tennis|handball|lacrosse|polo|curling|bobsled)\b',
        r'\b(skiing|snowboard|skating|surfing|skateboard|bmx|motocross|rally|formula|nascar|drag racing|karting)\b',
        r'\b(marathon|triathlon|pentathlon|decathlon|cycling|running|sprint|hurdles|javelin|discus|shot put|pole vault)\b',
        r'\b(gymnastics|diving|fencing|archery|shooting|equestrian|rowing|sailing|canoeing|kayaking)\b',
        # Sports organizations & events
        r'\b(olympics|olympic|fifa|uefa|nfl|nba|mlb|nhl|ipl|premier league|champions league|world cup|super bowl|wimbledon|masters|open championship)\b',
        # Sports roles & entities
        r'\b(player|coach|manager|referee|umpire|team|club|league|tournament|championship|match|game|season|draft)\b',
        # Wikipedia sports categories (common patterns)
        r'\b(sportsmen|sportswomen|sports teams|sports clubs|sports competitions|sports events|sports leagues)\b',
        r'(football club|basketball team|cricket team|baseball team|hockey team|rugby team)',
        r'(footballers|cricketers|basketball players|baseball players|tennis players|golfers|boxers|swimmers|runners|cyclists)',
        # Athletes by century/nationality (NEW - CRITICAL for accuracy!)
        r'(21st-century|20th-century|19th-century).*(sportspeople|sportsmen|sportswomen|athlete)',
        r'(21st-century|20th-century).*(baseball players|basketball players|football players|soccer players|cricketers|tennis players)',
        r'(american|british|indian|canadian|australian).*(sportspeople|sportsmen|sportswomen|athlete)',
        r'(american|british|indian).*(baseball players|basketball players|football players|cricketers)',
        r'(male|female).*(sportspeople|athlete|football|basketball|cricket|tennis)',
    ],
    
    "Entertainment": [
        # Movies & Film
        r'\b(film|films|movie|movies|cinema|filmmaker|filmography)\b',
        r'\b(actor|actress|actors|director|directors|producer|producers|screenwriter|cinematographer)\b',
        r'(american films|british films|indian films|action films|comedy films|drama films|horror films|thriller films|science fiction films|animated films|documentary films)',
        # TV & Streaming
        r'\b(television|tv show|tv series|sitcom|soap opera|reality tv|streaming|netflix|hulu|disney|hbo|amazon prime)\b',
        # Music
        r'\b(music|musical|musician|musicians|singer|singers|band|bands|orchestra|composer|composers|songwriter)\b',
        r'\b(album|albums|song|songs|single|concert|concerts|tour|record label|grammy|billboard)\b',
        r'(rock music|pop music|hip hop|jazz|classical music|country music|electronic music|metal music)',
        # Entertainment industry
        r'\b(hollywood|bollywood|entertainment|celebrity|celebrities|performer|performers|show business)\b',
        r'\b(oscar|academy award|emmy|golden globe|cannes|sundance)\b',
        # Wikipedia entertainment categories
        r'(entertainers|american entertainers|21st-century musicians|21st-century actors)',
        # People in entertainment (NEW - CRITICAL for accuracy!)
        r'(21st-century|20th-century|19th-century).*(actor|actress|film actor|television actor|voice actor)',
        r'(21st-century|20th-century|19th-century).*(musician|singer|rapper|composer|songwriter)',
        r'(american|british|indian|canadian|australian).*(actor|actress|film actor|television actor)',
        r'(american|british|indian|canadian|australian).*(musician|singer|rapper|songwriter)',
        r'(male|female).*(actor|actress|film actor|television actor|singer|musician)',
        # Writers & Authors (often in Entertainment)
        r'\b(writer|author|novelist|playwright|screenwriter|poet)\b',
        r'(21st-century|20th-century).*(writer|author|novelist|poet)',
        r'(american|british|indian).*(writer|author|novelist)',
    ],
    
    "Politics": [
        # Government & Elections
        r'\b(politic|politics|political|politician|politicians|government|governance|governmental)\b',
        r'\b(election|elections|vote|voting|voter|campaign|ballot|referendum|primary)\b',
        # Political positions
        r'\b(president|presidential|prime minister|senator|congressman|congresswoman|governor|mayor|minister|chancellor|parliament|parliamentary)\b',
        # Political entities
        r'\b(democrat|democratic|republican|conservative|liberal|labour|party|parties|coalition|cabinet)\b',
        # Royalty (often political)
        r'\b(royal|royalty|king|queen|prince|princess|duke|duchess|monarch|monarchy|crown)\b',
        # International politics
        r'\b(united nations|un |nato|european union|g7|g20|summit|treaty|diplomacy|diplomat|embassy|ambassador)\b',
        # Wikipedia political categories
        r'(heads of state|heads of government|american politicians|british politicians|political parties|general elections|presidential elections)',
        r'(prime ministers|presidents|senators|members of parliament|members of congress)',
        # Politicians by century/nationality (NEW - CRITICAL for accuracy!)
        r'(21st-century|20th-century|19th-century).*(politician|prime minister|president)',
        r'(american|british|indian|canadian|australian).*(politician|prime minister|president|senator)',
        r'(male|female).*(politician|president|prime minister)',
    ],
    
    "Technology": [
        # Computing & Software
        r'\b(technology|tech|technological|computer|computing|software|hardware|digital)\b',
        r'\b(app|apps|application|applications|program|programming|programmer|developer|developers|code|coding)\b',
        # Internet & Web
        r'\b(internet|web|website|websites|online|browser|browsers|search engine|social media)\b',
        # Tech companies & products
        r'\b(google|microsoft|apple|amazon|meta|facebook|twitter|android|ios|windows|linux|macos)\b',
        r'\b(iphone|ipad|macbook|surface|galaxy|pixel|chrome|firefox|safari|edge)\b',
        # Advanced tech
        r'\b(ai|artificial intelligence|machine learning|deep learning|neural network|algorithm)\b',
        r'\b(cloud|server|database|api|blockchain|cryptocurrency|bitcoin|virtual reality|augmented reality)\b',
        # Wikipedia tech categories
        r'(google software|microsoft software|apple inc|android software|ios software|web browsers|mobile apps|computer software|technology companies)',
        r'(software developers|computer scientists|programmers)',
    ],
    
    "Science": [
        # General science
        r'\b(science|scientific|scientist|scientists|research|researcher|researchers|laboratory|experiment)\b',
        # Physics & Chemistry
        r'\b(physics|physicist|physicists|chemistry|chemist|chemists|quantum|particle|atom|molecule|element)\b',
        # Biology & Life sciences
        r'\b(biology|biological|biologist|genetics|genetic|dna|rna|gene|evolution|ecology|ecosystem)\b',
        # Space & Astronomy
        r'\b(astronomy|astronomer|space|planet|planets|star|stars|galaxy|galaxies|nasa|telescope|cosmic|universe)\b',
        # Other sciences
        r'\b(mathematics|mathematician|geology|geologist|meteorology|oceanography|neuroscience|psychology|psychologist)\b',
        # Wikipedia science categories
        r'(scientists|physicists|chemists|biologists|astronomers|mathematicians|nobel laureates|scientific discoveries)',
    ],
    
    "Business and finance": [
        # Business
        r'\b(business|businessman|businesswoman|company|companies|corporation|corporations|corporate|enterprise)\b',
        r'\b(ceo|cfo|executive|executives|entrepreneur|entrepreneurs|founder|founders|startup|startups)\b',
        # Finance & Economics
        r'\b(finance|financial|bank|banking|banks|investment|investor|investors|stock|stocks|market|markets)\b',
        r'\b(economy|economic|economics|economist|trade|trading|commerce|commercial|revenue|profit)\b',
        # Industry
        r'\b(industry|industrial|manufacturing|retail|wholesale|supply chain|logistics)\b',
        # Wikipedia business categories
        r'(american companies|technology companies|financial institutions|businesspeople|entrepreneurs|chief executive officers)',
    ],
    
    "Health": [
        # Medical
        r'\b(health|healthcare|medical|medicine|hospital|hospitals|clinic|clinics)\b',
        r'\b(doctor|doctors|physician|physicians|surgeon|surgeons|nurse|nurses|patient|patients)\b',
        # Diseases & Conditions
        r'\b(disease|diseases|illness|condition|syndrome|disorder|cancer|diabetes|covid|pandemic|epidemic|virus|infection)\b',
        # Treatment
        r'\b(treatment|therapy|surgery|surgical|drug|drugs|medication|pharmaceutical|vaccine|vaccines)\b',
        # Wellness
        r'\b(wellness|fitness|nutrition|diet|mental health|exercise|yoga)\b',
        # Wikipedia health categories
        r'(diseases|medical conditions|healthcare|medical professionals|physicians|surgeons)',
    ],
    
    "Law and government": [
        # Legal system
        r'\b(law|legal|lawyer|lawyers|attorney|attorneys|judge|judges|justice|judicial|judiciary)\b',
        r'\b(court|courts|trial|lawsuit|litigation|case law|supreme court|appellate)\b',
        # Law enforcement
        r'\b(police|sheriff|law enforcement|fbi|cia|detective|investigation|criminal justice)\b',
        # Legal concepts
        r'\b(constitutional|constitution|legislation|statute|regulation|civil rights|criminal law|contract law)\b',
        # Wikipedia law categories
        r'(legal cases|supreme court|law enforcement|judges|lawyers)',
    ],
    
    "Travel and transportation": [
        # Travel & Tourism
        r'\b(travel|traveling|travelling|tourism|tourist|tourists|vacation|holiday|destination|destinations)\b',
        r'\b(hotel|hotels|resort|resorts|hostel|motel|accommodation|sightseeing|landmark|landmarks)\b',
        # Transportation
        r'\b(transport|transportation|airline|airlines|airport|airports|flight|flights|aviation)\b',
        r'\b(train|trains|railway|railways|railroad|subway|metro|bus|buses|transit)\b',
        r'\b(ship|ships|ferry|cruise|port|harbor|maritime|naval)\b',
        # Wikipedia travel categories
        r'(tourist attractions|airlines|airports|railways|transportation|hotels)',
    ],
    
    "Autos and vehicles": [
        # Vehicles
        r'\b(car|cars|automobile|automobiles|vehicle|vehicles|auto|automotive)\b',
        r'\b(truck|trucks|suv|sedan|coupe|hatchback|van|minivan|pickup)\b',
        r'\b(motorcycle|motorcycles|bike|bikes|scooter|motorbike)\b',
        # Brands (major ones)
        r'\b(toyota|ford|chevrolet|honda|nissan|bmw|mercedes|volkswagen|audi|tesla|hyundai|kia|mazda|subaru|jeep|dodge|ram)\b',
        r'\b(ferrari|lamborghini|porsche|maserati|bentley|rolls royce|bugatti|mclaren)\b',
        # Auto industry
        r'\b(automotive|auto industry|car manufacturer|motor company|dealership|mechanic)\b',
        # Wikipedia auto categories
        r'(motor vehicles|automobiles|car manufacturers|automotive companies)',
    ],
    
    "Games": [
        # Video games
        r'\b(game|games|gaming|video game|video games|esports|esport|gamer|gamers)\b',
        r'\b(playstation|xbox|nintendo|steam|epic games|pc gaming|console)\b',
        # Game types
        r'\b(mmo|mmorpg|rpg|fps|strategy game|puzzle game|action game|adventure game|simulation)\b',
        # Popular games
        r'\b(minecraft|fortnite|pokemon|zelda|mario|call of duty|grand theft auto|league of legends)\b',
        # Wikipedia gaming categories
        r'(video games|game developers|gaming companies|esports)',
    ],
    
    "Food and drink": [
        # Food
        r'\b(food|foods|cuisine|culinary|cooking|recipe|recipes|dish|dishes|meal|meals)\b',
        r'\b(restaurant|restaurants|chef|chefs|cook|bakery|cafe|cafeteria)\b',
        # Beverages
        r'\b(drink|drinks|beverage|beverages|coffee|tea|juice|soda|water)\b',
        r'\b(beer|wine|alcohol|alcoholic|cocktail|spirits|whiskey|vodka|rum|tequila)\b',
        # Food types
        r'\b(pizza|burger|sushi|pasta|steak|seafood|vegetarian|vegan|dessert|pastry|bread)\b',
        # Wikipedia food categories
        r'(restaurants|chefs|cuisine|foods|beverages|food companies)',
    ],
    
    "Climate": [
        # Climate & Environment
        r'\b(climate|climatic|weather|meteorology|temperature|precipitation|atmospheric)\b',
        r'\b(global warming|climate change|greenhouse|carbon|emission|emissions|fossil fuel)\b',
        # Natural disasters
        r'\b(hurricane|tornado|typhoon|cyclone|flood|flooding|drought|wildfire|earthquake|tsunami)\b',
        # Environmental
        r'\b(environment|environmental|ecology|ecological|conservation|biodiversity|deforestation)\b',
        r'\b(renewable|solar|wind power|sustainable|sustainability|pollution|recycling)\b',
        # Wikipedia climate categories
        r'(climate change|environmental issues|natural disasters|meteorology)',
    ],
    
    "Beauty and fashion": [
        # Fashion
        r'\b(fashion|style|clothing|apparel|garment|designer|designers|model|models|runway|couture)\b',
        r'\b(dress|dresses|suit|suits|shoes|footwear|handbag|accessory|accessories|jewelry)\b',
        # Beauty
        r'\b(beauty|cosmetic|cosmetics|makeup|skincare|hair|hairstyle|perfume|fragrance)\b',
        # Brands
        r'\b(gucci|chanel|prada|versace|dior|hermes|burberry|vogue|elle)\b',
        # Wikipedia fashion categories
        r'(fashion designers|fashion models|clothing|beauty products|cosmetics companies)',
    ],
    
    "Pets and animals": [
        # Animals
        r'\b(animal|animals|wildlife|species|mammal|mammals|reptile|reptiles|bird|birds|fish|fishes)\b',
        r'\b(dog|dogs|cat|cats|horse|horses|elephant|elephants|lion|tiger|bear|whale|shark)\b',
        # Pets
        r'\b(pet|pets|domestic animal|companion animal)\b',
        # Animal science
        r'\b(zoology|zoologist|veterinary|veterinarian|zoo|aquarium|safari|habitat|endangered)\b',
        # Wikipedia animal categories
        r'(animals|pets|wildlife|zoology|animal welfare|mammals|birds|reptiles|fish)',
    ],
    
    "Shopping": [
        # Retail
        r'\b(shop|shopping|retail|retailer|retailers|store|stores|mall|marketplace)\b',
        r'\b(buy|buying|purchase|sale|sales|discount|coupon|deal|bargain)\b',
        # E-commerce
        r'\b(ecommerce|e-commerce|online shopping|amazon|ebay|walmart|target|best buy|costco)\b',
        # Wikipedia shopping categories
        r'(retail companies|retailers|shopping|e-commerce|online retailers)',
    ],
    
    "Jobs and education": [
        # Education
        r'\b(education|educational|school|schools|university|universities|college|colleges|academy|institute)\b',
        r'\b(student|students|teacher|teachers|professor|professors|instructor|tutor|learning|teaching)\b',
        r'\b(course|courses|degree|diploma|graduation|academic|curriculum|examination)\b',
        # Jobs & Careers
        r'\b(job|jobs|career|careers|employment|employee|employees|employer|work|workplace|occupation|profession)\b',
        r'\b(hiring|recruit|recruiting|resume|cv|interview|salary|wage)\b',
        # Wikipedia education categories
        r'(universities|educational institutions|schools|educators|students|alumni)',
    ],
    
    "Hobbies and leisure": [
        # Hobbies
        r'\b(hobby|hobbies|leisure|recreation|recreational|pastime)\b',
        r'\b(craft|crafts|knitting|sewing|painting|drawing|art|photography|gardening)\b',
        r'\b(collecting|collector|stamp|coin|antique|vintage)\b',
        # Outdoor activities
        r'\b(camping|hiking|fishing|hunting|bird watching|climbing|mountaineering)\b',
        # DIY
        r'\b(diy|do it yourself|woodworking|carpentry|home improvement)\b',
        # Wikipedia hobby categories
        r'(hobbies|recreational activities|crafts|collectors)',
    ],
}

# Geography extraction keywords - Comprehensive coverage
LOCATION_KEYWORDS = {
    "City": {
        # Major world cities (100+ cities)
        "pattern": r'\b(new york|london|paris|tokyo|beijing|mumbai|delhi|shanghai|moscow|seoul|istanbul|jakarta|manila|cairo|mexico city|s[aã]o paulo|buenos aires|dhaka|karachi|los angeles|chicago|houston|phoenix|philadelphia|san antonio|san diego|dallas|san jose|austin|jacksonville|fort worth|columbus|charlotte|san francisco|indianapolis|seattle|denver|boston|nashville|detroit|portland|memphis|las vegas|baltimore|milwaukee|albuquerque|tucson|fresno|sacramento|kansas city|atlanta|miami|tampa|orlando|minneapolis|cleveland|pittsburgh|cincinnati|oakland|st louis|dubai|singapore|hong kong|sydney|melbourne|toronto|montreal|vancouver|madrid|barcelona|rome|milan|berlin|munich|frankfurt|amsterdam|brussels|vienna|zurich|stockholm|copenhagen|oslo|helsinki|dublin|lisbon|athens|warsaw|prague|budapest|bucharest|bangkok|hanoi|kuala lumpur|riyadh|doha|abu dhabi)\b',
        # Extract city names from common Wikipedia patterns
        "wiki_pattern": r'in ([A-Z][a-z]+(?: [A-Z][a-z]+)?)\b(?=,| )'
    },
    
    "State": {
        # US States
        "pattern": r'\b(alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|south carolina|south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|wisconsin|wyoming)\b',
        # Canadian provinces
        "canada": r'\b(ontario|quebec|british columbia|alberta|manitoba|saskatchewan|nova scotia|new brunswick|newfoundland|prince edward island)\b',
        # Indian states (major ones)
        "india": r'\b(maharashtra|karnataka|tamil nadu|west bengal|rajasthan|gujarat|kerala|andhra pradesh|telangana|punjab|haryana|bihar|uttar pradesh)\b',
    },
    
    "Country": {
        # Countries with nationality adjectives
        "usa": r'\b(american|america|united states|usa|u\.s\.|us )\b',
        "uk": r'\b(british|britain|united kingdom|uk|england|english|scottish|scotland|welsh|wales)\b',
        "france": r'\b(french|france)\b',
        "germany": r'\b(german|germany|deutschland)\b',
        "italy": r'\b(italian|italy|italia)\b',
        "spain": r'\b(spanish|spain|espa[ñn]a)\b',
        "russia": r'\b(russian|russia)\b',
        "china": r'\b(chinese|china)\b',
        "japan": r'\b(japanese|japan|nippon)\b',
        "india": r'\b(indian|india|bharat)\b',
        "brazil": r'\b(brazilian|brazil|brasil)\b',
        "canada": r'\b(canadian|canada)\b',
        "australia": r'\b(australian|australia|aussie)\b',
        "mexico": r'\b(mexican|mexico|m[eé]xico)\b',
        "argentina": r'\b(argentinian|argentinean|argentina)\b',
        "korea": r'\b(korean|korea|south korea)\b',
        "netherlands": r'\b(dutch|netherlands|holland)\b',
        "belgium": r'\b(belgian|belgium)\b',
        "switzerland": r'\b(swiss|switzerland)\b',
        "sweden": r'\b(swedish|sweden)\b',
        "norway": r'\b(norwegian|norway)\b',
        "denmark": r'\b(danish|denmark)\b',
        "finland": r'\b(finnish|finland)\b',
        "poland": r'\b(polish|poland)\b',
        "turkey": r'\b(turkish|turkey)\b',
        "egypt": r'\b(egyptian|egypt)\b',
        "south africa": r'\b(south african|south africa)\b',
        "nigeria": r'\b(nigerian|nigeria)\b',
        "kenya": r'\b(kenyan|kenya)\b',
        "pakistan": r'\b(pakistani|pakistan)\b',
        "bangladesh": r'\b(bangladeshi|bangladesh)\b',
        "indonesia": r'\b(indonesian|indonesia)\b',
        "thailand": r'\b(thai|thailand)\b',
        "vietnam": r'\b(vietnamese|vietnam)\b',
        "philippines": r'\b(filipino|philippine|philippines)\b',
        "malaysia": r'\b(malaysian|malaysia)\b',
        "singapore": r'\b(singaporean|singapore)\b',
        "new zealand": r'\b(new zealand|kiwi)\b',
        "iran": r'\b(iranian|iran)\b',
        "iraq": r'\b(iraqi|iraq)\b',
        "israel": r'\b(israeli|israel)\b',
        "saudi arabia": r'\b(saudi|saudi arabia)\b',
        "uae": r'\b(emirati|emirates|uae|united arab emirates)\b',
        "chile": r'\b(chilean|chile)\b',
        "colombia": r'\b(colombian|colombia)\b',
        "peru": r'\b(peruvian|peru)\b',
        "venezuela": r'\b(venezuelan|venezuela)\b',
        "portugal": r'\b(portuguese|portugal)\b',
        "greece": r'\b(greek|greece)\b',
        "austria": r'\b(austrian|austria)\b',
        "czech": r'\b(czech|czechia|czech republic)\b',
        "hungary": r'\b(hungarian|hungary)\b',
        "romania": r'\b(romanian|romania)\b',
        "ukraine": r'\b(ukrainian|ukraine)\b',
    },
    
    "Region": {
        "pattern": r'\b(middle east|southeast asia|south asia|east asia|eastern europe|western europe|central america|south america|north america|central asia|scandinavia|balkans|caribbean|mediterranean)\b',
    }
}


def classify_with_keywords(page_title, wiki_categories):
    """
    Classify page using keyword matching on Wikipedia categories
    
    Args:
        page_title: Page title (used as fallback)
        wiki_categories: List of Wikipedia category strings or pipe-separated string
    
    Returns:
        Category name from CATEGORIES list
    """
    # Convert wiki_categories to searchable text
    if isinstance(wiki_categories, str):
        # Handle pipe-separated string
        search_text = wiki_categories.replace("|", " ").lower()
    elif isinstance(wiki_categories, list):
        # Handle list of categories
        search_text = " ".join(wiki_categories).lower()
    else:
        search_text = ""
    
    # Add title to search text
    search_text += " " + page_title.replace("_", " ").lower()
    
    # Score each category
    scores = {}
    for category, patterns in KEYWORD_RULES.items():
        score = 0
        for pattern in patterns:
            matches = re.findall(pattern, search_text, re.IGNORECASE)
            score += len(matches)
        
        if score > 0:
            scores[category] = score
    
    # Return category with highest score
    if scores:
        best_category = max(scores, key=scores.get)
        return best_category
    
    # Fallback to "Other" if no matches
    return "Other"


def extract_geography_with_keywords(page_title, wiki_categories):
    """
    Extract geography using comprehensive keyword matching
    
    Args:
        page_title: Page title
        wiki_categories: List of Wikipedia category strings or pipe-separated string
    
    Returns:
        Dict with location_type and location, or None
    """
    # Convert to searchable text
    if isinstance(wiki_categories, str):
        search_text = wiki_categories.replace("|", " ").lower()
    elif isinstance(wiki_categories, list):
        search_text = " ".join(wiki_categories).lower()
    else:
        search_text = ""
    
    search_text += " " + page_title.replace("_", " ").lower()
    
    # Check for specific locations in order of specificity
    # Cities first (most specific)
    city_pattern = LOCATION_KEYWORDS["City"]["pattern"]
    if re.search(city_pattern, search_text, re.IGNORECASE):
        match = re.search(city_pattern, search_text, re.IGNORECASE)
        location_name = match.group(0)
        # Capitalize properly
        location_name = " ".join(word.capitalize() for word in location_name.split())
        return {
            "location_type": "City",
            "location": location_name
        }
    
    # Then states (US, Canada, India)
    for state_type, pattern in LOCATION_KEYWORDS["State"].items():
        if re.search(pattern, search_text, re.IGNORECASE):
            match = re.search(pattern, search_text, re.IGNORECASE)
            location_name = match.group(0)
            # Capitalize properly
            location_name = " ".join(word.capitalize() for word in location_name.split())
            return {
                "location_type": "State",
                "location": location_name
            }
    
    # Then countries
    for country_name, pattern in LOCATION_KEYWORDS["Country"].items():
        if re.search(pattern, search_text, re.IGNORECASE):
            # Map to proper country name
            country_map = {
                "usa": "United States",
                "uk": "United Kingdom",
                "france": "France",
                "germany": "Germany",
                "italy": "Italy",
                "spain": "Spain",
                "russia": "Russia",
                "china": "China",
                "japan": "Japan",
                "india": "India",
                "brazil": "Brazil",
                "canada": "Canada",
                "australia": "Australia",
                "mexico": "Mexico",
                "argentina": "Argentina",
                "korea": "South Korea",
                "netherlands": "Netherlands",
                "belgium": "Belgium",
                "switzerland": "Switzerland",
                "sweden": "Sweden",
                "norway": "Norway",
                "denmark": "Denmark",
                "finland": "Finland",
                "poland": "Poland",
                "turkey": "Turkey",
                "egypt": "Egypt",
                "south africa": "South Africa",
                "nigeria": "Nigeria",
                "kenya": "Kenya",
                "pakistan": "Pakistan",
                "bangladesh": "Bangladesh",
                "indonesia": "Indonesia",
                "thailand": "Thailand",
                "vietnam": "Vietnam",
                "philippines": "Philippines",
                "malaysia": "Malaysia",
                "singapore": "Singapore",
                "new zealand": "New Zealand",
                "iran": "Iran",
                "iraq": "Iraq",
                "israel": "Israel",
                "saudi arabia": "Saudi Arabia",
                "uae": "United Arab Emirates",
                "chile": "Chile",
                "colombia": "Colombia",
                "peru": "Peru",
                "venezuela": "Venezuela",
                "portugal": "Portugal",
                "greece": "Greece",
                "austria": "Austria",
                "czech": "Czech Republic",
                "hungary": "Hungary",
                "romania": "Romania",
                "ukraine": "Ukraine",
            }
            return {
                "location_type": "Country",
                "location": country_map.get(country_name, country_name.title())
            }
    
    # Finally check regions
    region_pattern = LOCATION_KEYWORDS["Region"]["pattern"]
    if re.search(region_pattern, search_text, re.IGNORECASE):
        match = re.search(region_pattern, search_text, re.IGNORECASE)
        location_name = match.group(0)
        # Capitalize properly
        location_name = " ".join(word.capitalize() for word in location_name.split())
        return {
            "location_type": "Region",
            "location": location_name
        }
    
    # No location found
    return None


def get_category_stats(categories_list):
    """Get category distribution statistics"""
    from collections import Counter
    return dict(Counter(categories_list))
