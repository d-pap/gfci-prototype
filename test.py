import itertools        
import re

# JOBS = ['data analyst']
# CITIES = ['Chicago, IL', 'Detroit, MI']

# combinations = list(itertools.product(JOBS, CITIES))
# print(combinations)

# jr job
desc1 = "Job Summary: The Data Analyst I will deliver insights, support business leaders, and assist in procuring data for functional analytics. The role will partner with business units to help ask critical questions and drive success. The Data Analyst will act as a storyteller who collects data from various sources to guide strategic business decisions. From optimizing reporting to solving ad hoc questions to deep investigation and study, the position will play a key component in helping make great de…"
# mid job
desc2 = "R1 is the leading provider of technology-driven solutions that transform the patient experience and financial performance of hospitals, health systems and medical groups. We are the one company that combines the deep expertise of a global workforce of revenue cycle professionals with the industry's most advanced technology platform, encompassing sophisticated analytics, AI, intelligent automation, and workflow orchestration.? As our Data Operations Analyst II, you will be responsible for managi…"
# sr job
desc3 = "Administrative Assistant (4875) Location Detroit, MI Job Code 4875  of Openings Apply Now (https://phg.tbe.taleo.net/phg01/ats/careers/v2/applyRequisition?org=GATEWAYVENT&cws=55&rid=4875) Job Brief Bennett Aerospace, Inc. has an opening for a highly motivated Data Analyst IV in Cincinatti, Ohio (3146) Bennett Aerospace Inc., a subsidiary of Three Saints Bay, LLC and a Federal Government Contractor industry leader, has an opening for a highly motivated Administrative Assistant located in Detroit…"

# titles
title1 = "Data Analyst I"
title2 = "Data Operations Analyst II"
title3 = "Sr. Analyst"
title4 = "Senior Analyst"
title5 = 'data lead'
title6 = 'associate director'

txt = " ".join([title1, desc1]).lower()
print(txt)
print('-'*100)

txt = " ".join([title2, desc2]).lower()
print(txt)
print('-'*100)

def check_title_keywords(title):
    role = title.lower()
    senior_role_tags = ('sr', 'senior', 'iv', 'lead', 'principal', 'director', 'head', 'vp', 'executive', 'president')
    mid_tags = ('ii', 'experienced', 'manager', 'iii')
    junior_role_tags = ('jr', 'associate', 'junior', 'intern', 'entry level')
    
    if any(tag in role for tag in senior_role_tags):    
        return "sr"
    if any(tag in role for tag in mid_tags):
        return "mid"
    return "jr"

def check_salary_range(salary_min, salary_max):
    """
    Categorize salary range into seniority level
    Returns: 'jr', 'mid', or 'sr'
    
    ! SHOULD BE REPLACED WITH A MORE ROBUST SALARY RANGE CHECK AND TAILORING THE CHECK TO THE CITY-------------------------------------------------
    """
    salary_avg = (salary_min + salary_max) / 2
    
    
    if salary_min < 74000:
        return "jr"
    elif salary_min > 100000:
        return "sr"
    else:
        return "mid"
    
def check_description_keywords(txt):

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
            if years >= 7:
                return "sr"
            return "mid"


def categorize_role(title, description, salary_min, salary_max, city):
    role = title.lower()
    desc = description.lower()
    txt = " ".join([role or "", desc or ""]).lower()
    
    title_seniority = check_title_keywords(title)
    if title_seniority:
        return title_seniority
    
    if salary_min and salary_max:
        salary_seniority = check_salary_range(salary_min, salary_max)
        if salary_seniority:
            return salary_seniority
    
    if description:
        description_seniority = check_description_keywords(description)


test1 = categorize_role(title5, desc2, 100000, 120000, 'Chicago, IL')
print(test1)

def categorize_seniority(title, description=""):
    """
    Categorize job seniority based on title and description
    Returns: 'jr', 'mid', or 'sr'
    """
    # NEW VERSION TO CLASSIFY SENIORITY LEVEL:
    # 1) look for explicit YOE (including ranges)
    # 2) if none, scan title for level indicators
    # 3) default to mid
    
    role = title.lower()
    desc = description.lower()
    
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
