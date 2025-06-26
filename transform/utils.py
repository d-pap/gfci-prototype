import re

# In transform/utils.py
def get_state_abbreviation(state_input):
    """
    Convert state name to abbreviation
    Handles both full names and abbreviations
    
    Args:
        state_input: "Illinois", "IL", "illinois", etc.
    
    Returns:
        "IL" or original input if not found
    """
    
    # State name to abbreviation mapping
    state_name_to_abbrev = {
        'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
        'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
        'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
        'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
        'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
        'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
        'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
        'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM',
        'new york': 'NY', 'north carolina': 'NC', 'north dakota': 'ND',
        'ohio': 'OH', 'oklahoma': 'OK', 'oregon': 'OR', 'pennsylvania': 'PA',
        'rhode island': 'RI', 'south carolina': 'SC', 'south dakota': 'SD',
        'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT', 'vermont': 'VT',
        'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
        'wisconsin': 'WI', 'wyoming': 'WY', 'district of columbia': 'DC',
    }
    
    if not state_input:
        return ''
    
    # Normalize input (lowercase, strip whitespace)
    normalized_input = state_input.lower().strip()
    
    # Check if it's already an abbreviation
    if len(normalized_input) == 2 and normalized_input.upper() in state_name_to_abbrev.values():
        return normalized_input.upper()
    
    # Try to find full state name
    return state_name_to_abbrev.get(normalized_input, state_input)

def categorize_seniority(title, description=""):
    """
    Categorize job seniority based on title and description
    Returns: 'jr', 'mid', or 'sr'
    """
    # NEW VERSION TO CLASSIFY SENIORITY LEVEL:
    # 1) look for explicit YOE (including ranges)
    # 2) if none, scan title for level indicators
    # 3) default to mid
    
    txt = " ".join([title or "", description or ""]).lower()
    
    # 1) regex for YOE
    # catches "2 years", "3-5 years", "4+ yrs", or "at least 7 years"
    patterns = [
        r'(\d+)\s*-\s*(\d+)\s*years?',
        r'(\d+)\+?\s*(?:years?|yrs?|yos?)',
        r'at\s*least\s*(\d+)\s*years?', 
        r'minimum\s*(?:of\s*)?(\d+)\s*years?'
    ]
    
    for pat in patterns:
        m = re.search(pat, txt)
        if m:
            # if range, take the lower bound
            years = int(m.group(1))
            if years <= 3:
                return "jr"
            if years <= 5:
                return "mid"
            return "sr"
        
    # 2) look for numeric "level" in title (I, II, III, etc.
    if re.search(r'\b(level\s*(i|ii|iii|iv|v))\b', title.lower()):
        lvl = title.lower().split("level")[-1].strip()
        if lvl in ("i", "1"):   return "jr"
        if lvl in ("ii","2"):   return "mid"
        return "sr"
    
    # 3) fallback title hints 
    senior_tags = ('sr', 'lead', 'principal', 'director', 'head', 'vp', 'executive', 'team lead')
    if any(tag in txt for tag in senior_tags):
        return "sr"
    junior_tags = ('jr', 'associate', 'junior', 'intern')
    if any(tag in title.lower() for tag in junior_tags):
        return "jr"
    
    # 4) default to mid
    return "mid"

def get_is_remote(title, description=""):
    """
    Check if job is remote based on title and description   
    """
    remote_keywords = ['remote', 'work from home', 'wfh']
    txt = " ".join([title or "", description or ""]).lower()
    return any(kw in txt for kw in remote_keywords)

def get_industry(title, company, category):
    """
    Get industry from title, company, and category
    """
    
    # TODO: infer industry - look for domain hints
        # 'nurse', 'clinical', 'hospital' -> healthcare
        # 'software', 'engineer', 'developer' -> tech
    #! build a small internal dictionary or keyword map of roles -> industries or company names -> industries
    #! fallback to category if no match
    

    # WHEN company ~* '\b(google|microsoft|amazon|apple|meta|netflix|tesla|uber|airbnb)\b' THEN 'big_tech'
    # WHEN company ~* '\b(jpmorgan|goldman|morgan stanley|wells fargo|bank of america|citi)\b' THEN 'finance'
    # WHEN company ~* '\b(mckinsey|bain|bcg|deloitte|pwc|accenture|kpmg)\b' THEN 'consulting'
    # WHEN company ~* '\b(pfizer|merck|johnson|roche|novartis|astrazeneca)\b' THEN 'pharma'
    # WHEN title ~* '\bfintech\b' OR company ~* '\b(stripe|square|robinhood|coinbase)\b' THEN 'fintech'
    # WHEN title ~* '\bhealthcare?\b' OR company ~* '\b(health|medical|hospital)\b' THEN 'healthcare'
    # WHEN company ~* '\b(startup|inc\.?|llc)\b' AND 
    #      (SELECT COUNT(*) FROM jobs_enriched WHERE company = jobs_enriched.company) < 10 THEN 'startup'
    # ELSE 'other'

    
    pass

def get_job_type(title, description=""):
    """
    Get job type from description
    """
    
    # TODO: search desc and title for signals:
        # look in title and desc: part-time/part time/pt = part time, full-time/full time/ft = full time, etc.
        # look for hours in desc (e.g. 20 hours/week)
    
    if 'intern' in title.lower() or 'internship' in title.lower():
        return 'intern'
    if 'part time' in description.lower():
        return 'part-time'
    if 'full time' in description.lower():
        return 'full-time'
    if 'contract' in description.lower():
        return 'contract'
    return 'full-time'

def get_yoe(description=""):
    """
    Estimate years of experience based on title and description
    """
    
    # TODO: use regex to extract yoe from desc bc there's many edge cases
        # X+ year, X yrs., minimum X years, at least X years, etc.
        # normalize to integers (3+ years -> 3)
        # look for ranges (2-3 years -> 2)
    #! use regex to extract numbers around the words "year" or "years"
    #! ignore things like "5 years in business", "over 20 years of company experience", etc. 

    pass

def get_education(description=""):
    """
    Get education from title and description
    """
    
    # TODO: use regex to extract education from desc
        # "bachelor's degree required", "BA/BS in CS or related", "master's or PhD preferred", etc.
        # consider "degree in progress" (internship), "BS req, MS preferred", etc.
    
    pass

def get_cbsa_code(location):
    pass

'''
Functions to standardize job data in the silver schema
'''
def standardize_location(city, county, state, location_raw, source):
    """
    Standardize location to: "City, State" format
    Extract county separately
    """
    
    if source == 'adzuna':
        # Adzuna: location might be "Chicago, Cook County"
        # We already have city, county, state separately
        standardized_location = f"{city}, {state}" if city and state else location_raw
        standardized_county = county
        
    elif source == 'jsearch':
        # JSearch: location is "Chicago, IL", no county info
        standardized_location = location_raw  # Already in correct format
        standardized_county = ""  # JSearch doesn't provide county
    
    return {
        'location': standardized_location,  # "Chicago, IL"
        'county': standardized_county       # "Cook County" or ""
    }
    
# def standardize_category(category_raw, title, description, source):
#     """
#     Standardize job categories across sources
#     """
    
#     if source == 'adzuna' and category_raw:
#         # Clean up Adzuna categories
#         return clean_adzuna_category(category_raw)
    
#     elif source == 'jsearch':
#         # Infer category from title and description
#         return infer_category_from_content(title, description)
    
#     return "Other"  # Default fallback

# def clean_adzuna_category(category):
#     """Clean Adzuna category format"""
#     # "IT Jobs" -> "Technology"
#     # "Retail Jobs" -> "Retail"
#     category_mapping = {
#         "IT Jobs": "Technology",
#         "Retail Jobs": "Retail", 
#         "Finance Jobs": "Finance",
#         "Healthcare Jobs": "Healthcare",
#         # ... add more mappings
#     }
    
#     return category_mapping.get(category, category.replace(" Jobs", ""))

# def infer_category_from_content(title, description):
#     """Infer category from job title and description"""
#     title_lower = title.lower()
#     desc_lower = description.lower()
    
#     # Technology keywords
#     if any(keyword in title_lower for keyword in ['data', 'analyst', 'engineer', 'developer', 'programmer']):
#         return "Technology"
    
#     # Finance keywords  
#     if any(keyword in title_lower for keyword in ['finance', 'accounting', 'financial']):
#         return "Finance"
    
#     # Add more inference logic...
#     return "Other"

def standardize_job_type(job_type_raw, title, description, source):
    """
    Standardize job types to consistent format
    """
    
    if source == 'adzuna':
        # Use existing inference logic
        return get_job_type(description)  # Returns "full-time", "part-time", etc.
    
    elif source == 'jsearch' and job_type_raw:
        return clean_jsearch_job_type(job_type_raw)
    
    return "full-time"  # Default assumption

def clean_jsearch_job_type(job_type_raw):
    """Clean JSearch job type format"""
    
    # Handle multiple types: "Full-time and Part-time" 
    if " and " in job_type_raw:
        # Take the first/primary type
        primary_type = job_type_raw.split(" and ")[0]
    else:
        primary_type = job_type_raw
    
    # Standardize to lowercase
    type_mapping = {
        "Full-time": "full-time",
        "Part-time": "part-time", 
        "Contract": "contract",
        "Internship": "internship",
        "Temporary": "temporary"
    }
    
    return type_mapping.get(primary_type, primary_type.lower())

# def log_standardization_stats():
#     """Track what standardization is happening"""
    
#     # Track category inference success rate
#     jsearch_jobs_with_inferred_category = count_jobs_where_category_inferred()
    
#     # Track description cleaning impact
#     descriptions_cleaned = count_descriptions_with_headers_removed()
    
#     # Track location standardization
#     location_format_consistency = check_location_format_consistency()
    
#     print(f"Standardization Stats:")
#     print(f"- JSearch categories inferred: {jsearch_jobs_with_inferred_category}")
#     print(f"- Descriptions cleaned: {descriptions_cleaned}")
#     print(f"- Location format consistency: {location_format_consistency}%")