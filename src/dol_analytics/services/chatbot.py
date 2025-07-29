import json
import psycopg2
import psycopg2.extras
import logging
from typing import Dict, Any
from openai import OpenAI

# Use relative imports if running as a module
try:
    from ..config import get_settings
except ImportError:
    # Use absolute imports if running as a script
    from src.dol_analytics.config import get_settings

# Set up logger
logger = logging.getLogger("dol_analytics.chatbot")


class PermChatbot:
    """
    Simple chatbot for PERM case queries. Extract params, ask for missing ones, or run query.
    """
    
    def __init__(self, db_connection):
        self.conn = db_connection
        
        # Get settings using the project's config pattern
        self.settings = get_settings()
        
        # Set up OpenAI client
        self.openai_client = None
        if self.settings.OPENAI_API_KEY:
            self.openai_client = OpenAI(api_key=self.settings.OPENAI_API_KEY)
        else:
            logger.warning("No OpenAI API key found. Set OPENAI_API_KEY in .env file.")
    
    def process_message(self, message: str) -> Dict[str, Any]:
        """
        Simple flow: classify intent and extract params in one AI call
        """
        try:
            # Use AI to classify and extract in one call
            result = self.analyze_message(message)
            
            if result["intent"] == "case_lookup":
                return self.handle_case_lookup()
            elif result["intent"] == "timeline_question":
                return self.handle_timeline_question()
            elif result["intent"] == "count_query":
                # Check if we have everything we need
                if self.has_complete_query(result["parameters"]):
                    return self.run_query(result["parameters"])
                else:
                    return self.ask_for_missing(result["parameters"], message)
            else:
                return self.handle_unknown()
                
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            return {
                "response": f"Sorry, I encountered an error: {str(e)}",
                "type": "error",
                "links": []
            }
    
    def analyze_message(self, message: str) -> Dict[str, Any]:
        """
        Enhanced AI analysis: classify intent AND extract parameters in one call
        """
        if not self.openai_client:
            return {"intent": "unknown", "parameters": {}}
        
        prompt = f"""
Analyze this user message for PERM case queries.

STEP 1: Classify the intent
- "case_lookup" - User wants to find their specific case number or case details (examples: "what is my case number", "find my case", "case status", "look up my case")
- "timeline_question" - User asks about processing time or when they'll be current (examples: "when will my case be certified", "when will I be current", "priority date timeline", "October 2024 letter H when current", "how long will it take")
- "count_query" - User wants count statistics by company letter and status (examples: "V pending in April 2024", "how many T approved", "S certified April 2024", "how many cases certified letter S April 2024", "N denied in March")
- "unknown" - Anything else

STEP 2: If count_query, extract these parameters:
- company_letter: Single letter A-Z (like V, T, N, S)
- status: pending/approved/certified/denied  
- month: month name
- year: 2024 or 2025

Message: "{message}"

Response format:
{{
    "intent": "case_lookup" | "timeline_question" | "count_query" | "unknown",
    "parameters": {{
        "company_letter": "V",
        "status": "pending",
        "month": "April", 
        "year": "2024"
    }}
}}

If parameters aren't found or intent isn't count_query, leave parameters empty.
"""

        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4.1-nano-2025-04-14",
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                max_tokens=150
            )
            
            result = json.loads(response.choices[0].message.content.strip())
            return result
            
        except Exception as e:
            logger.error(f"AI analysis error: {str(e)}")
            return {"intent": "unknown", "parameters": {}}
    
    def handle_unknown(self) -> Dict[str, Any]:
        """
        Handle unknown requests
        """
        return {
            "response": "I can help with PERM case counts (like 'V pending in April 2024') or finding your case. What would you like to know?",
            "type": "unknown",
            "links": [
                {
                    "text": "Case Search",
                    "url": "/case-search", 
                    "description": "Find your specific case"
                }
            ]
        }
    
    def handle_case_lookup(self) -> Dict[str, Any]:
        """
        Handle case lookup questions
        """
        return {
            "response": "I can help you find your PERM case! Use the case search tool to look up your case number and check its current status.",
            "type": "case_lookup",
            "links": [
                {
                    "text": "Case Search Tool",
                    "url": "/case-search",
                    "description": "Look up your PERM case number and status"
                }
            ]
        }
    
    def handle_timeline_question(self) -> Dict[str, Any]:
        """
        Handle timeline questions
        """
        return {
            "response": "I can help you estimate processing timelines! Use the timeline estimator to see when your priority date might become current or how long your case might take.",
            "type": "timeline_question",
            "links": [
                {
                    "text": "Timeline Estimator",
                    "url": "/#timeline-estimator",
                    "description": "Check when your priority date will be current"
                }
            ]
        }
    
    def has_complete_query(self, params: Dict[str, Any]) -> bool:
        """
        Check if we have all required parameters: company_letter, status, month, year
        """
        required = ["company_letter", "status", "month", "year"]
        return all(params.get(param) for param in required)
    
    def ask_for_missing(self, params: Dict[str, Any], message: str) -> Dict[str, Any]:
        """
        Ask for whatever parameters are missing
        """
        missing = []
        if not params.get("company_letter"):
            missing.append("company letter (like V, T, N)")
        if not params.get("status"):
            missing.append("status (pending, approved, certified, denied)")
        if not params.get("month"):
            missing.append("month")
        if not params.get("year"):
            missing.append("year (2024 or 2025)")
        
        if len(missing) == 1:
            response = f"I need the {missing[0]}. "
        elif len(missing) == 2:
            response = f"I need the {missing[0]} and {missing[1]}. "
        else:
            response = f"I need the {', '.join(missing[:-1])}, and {missing[-1]}. "
        
        response += "For example: 'V pending in April 2024'"
        
        return {
            "response": response,
            "type": "missing_params",
            "data": params,
            "links": []
        }
    
    def run_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the actual database query
        """
        company_letter = params["company_letter"].upper()
        status_word = params["status"].lower()
        month_name = params["month"].title()
        year = int(params["year"])
        
        # Map status to database values
        status_mapping = {
            'pending': 'ANALYST REVIEW',
            'approved': 'CERTIFIED', 
            'certified': 'CERTIFIED',
            'denied': 'DENIED',
            'withdrawn': 'WITHDRAWN'
        }
        db_status = status_mapping.get(status_word, 'ANALYST REVIEW')
        
        # Map month to number
        month_mapping = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12
        }
        month_num = month_mapping.get(month_name.lower())
        
        if not month_num:
            return {
                "response": f"I don't recognize '{month_name}' as a month.",
                "type": "error",
                "links": []
            }
        
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM perm_cases
                    WHERE status = %s
                    AND EXTRACT(MONTH FROM submit_date) = %s
                    AND EXTRACT(YEAR FROM submit_date) = %s
                    AND employer_first_letter = %s
                """, (db_status, month_num, year, company_letter))
                
                result = cursor.fetchone()
                count = result['count'] if result else 0
                
                return {
                    "response": f"There are {count:,} {company_letter} ({status_word}) PERM cases submitted in {month_name} {year}.",
                    "type": "count",
                    "data": {
                        "count": count,
                        "company_letter": company_letter,
                        "status": status_word,
                        "month": month_name,
                        "year": year
                    },
                    "links": []
                }
        except Exception as e:
            return {
                "response": f"Database error: {str(e)}",
                "type": "error",
                "links": []
            } 