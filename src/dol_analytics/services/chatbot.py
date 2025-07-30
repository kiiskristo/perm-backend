import json
import psycopg2
import psycopg2.extras
import logging
from typing import Dict, Any, Optional, List
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
            elif result["intent"] == "month_start_prediction":
                return self.handle_month_start_prediction(result["parameters"])
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
- "month_start_prediction" - User asks when a specific month will start being processed (examples: "when will May start", "when will June start", "when does April begin", "October 2024 when will it start", "when will DOL start working on July", "when does September start", "November 2024 start date")
- "count_query" - User wants count statistics by company letter and status (examples: "V pending in April 2024", "how many T approved", "S certified April 2024", "how many cases certified letter S April 2024", "N denied in March")
- "unknown" - Anything else

STEP 2: Extract parameters based on intent:
- For count_query: company_letter (A-Z), status (pending/approved/certified/denied), month, year
- For month_start_prediction: target_month (any month name: January, February, March, April, May, June, July, August, September, October, November, December), target_year (2024/2025)
- For other intents: leave parameters empty
  IMPORTANT: If the query mentions multiple conflicting months/years, classify as "unknown" instead

Message: "{message}"

Response format:
{{
    "intent": "case_lookup" | "timeline_question" | "month_start_prediction" | "count_query" | "unknown",
    "parameters": {{
        "company_letter": "V",
        "status": "pending", 
        "month": "April",
        "year": "2024",
        "target_month": "October",
        "target_year": "2024"
    }}
}}

If parameters aren't found or intent doesn't require them, leave parameters empty.
"""

        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-4.1-mini-2025-04-14",
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
            "response": "I can help with:\n• PERM case counts (like 'V pending in April 2024')\n• Finding your case status\n• Predicting when new months will start (like 'when will May start')\n• Processing timeline questions\n\nWhat would you like to know?",
            "type": "unknown",
            "links": [
                {
                    "text": "Case Search",
                    "url": "/case-search", 
                    "description": "Find your specific case"
                },
                {
                    "text": "Updated Cases",
                    "url": "/updated-cases",
                    "description": "View recently updated cases"
                },
                {
                    "text": "Timeline Estimator",
                    "url": "#timeline-estimator",
                    "description": "Estimate your case timeline"
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
    
    def handle_month_start_prediction(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle month start prediction questions
        """
        try:
            target_month = params.get("target_month", "").title()
            target_year = params.get("target_year")
            
            if not target_month:
                return {
                    "response": "I can predict when DOL will start processing new months! Please specify which month you're asking about (e.g., 'when will May start' or 'when will June 2024 start').",
                    "type": "month_start_prediction",
                    "links": []
                }
            
            # Default to current year if not specified
            if not target_year:
                from datetime import date
                target_year = str(date.today().year)
            
            # Get month start prediction
            prediction_result = self.predict_month_start(target_month, int(target_year))
            
            return {
                "response": prediction_result["message"],
                "type": "month_start_prediction", 
                "links": prediction_result.get("links", []),
                "data": prediction_result.get("data", {})
            }
            
        except Exception as e:
            logger.error(f"Error in month start prediction: {str(e)}")
            return {
                "response": f"Sorry, I couldn't predict when {target_month} will start. Error: {str(e)}",
                "type": "error",
                "links": []
            }
    
    def get_most_active_month(self) -> Optional[Dict[str, Any]]:
        """
        Find the current processing month - the earliest month with significant backlog (>3000).
        This represents the month DOL is currently working on most heavily.
        """
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                # Find the earliest 2024 month with backlog > 3000 (active processing threshold)
                cursor.execute("""
                    SELECT 
                        month,
                        year,
                        count as backlog_count
                    FROM monthly_status 
                    WHERE status = 'ANALYST REVIEW' 
                        AND year = 2024
                        AND count > 3000
                    ORDER BY 
                        CASE
                            WHEN month = 'January' THEN 1
                            WHEN month = 'February' THEN 2
                            WHEN month = 'March' THEN 3
                            WHEN month = 'April' THEN 4
                            WHEN month = 'May' THEN 5
                            WHEN month = 'June' THEN 6
                            WHEN month = 'July' THEN 7
                            WHEN month = 'August' THEN 8
                            WHEN month = 'September' THEN 9
                            WHEN month = 'October' THEN 10
                            WHEN month = 'November' THEN 11
                            WHEN month = 'December' THEN 12
                        END ASC
                    LIMIT 1
                """)
                
                result = cursor.fetchone()
                if result:
                    return {
                        "month": result["month"],
                        "year": result["year"],
                        "backlog_count": result["backlog_count"]
                    }
                return None
                
        except Exception as e:
            logger.error(f"Error getting most active month: {str(e)}")
            return None
    
    def get_month_backlog(self, month: str, year: int) -> Optional[int]:
        """
        Get the current backlog (ANALYST REVIEW count) for a specific month.
        """
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute("""
                    SELECT count 
                    FROM monthly_status 
                    WHERE month = %s 
                        AND year = %s 
                        AND status = 'ANALYST REVIEW'
                """, (month, year))
                
                result = cursor.fetchone()
                return result["count"] if result else None
                
        except Exception as e:
            logger.error(f"Error getting month backlog for {month} {year}: {str(e)}")
            return None
    
    def get_average_daily_processing_rate(self) -> float:
        """
        Calculate average weekly processing rate matching the dashboard's weekly_volumes calculation.
        Returns weekly rate using complete weeks only, excluding partial current week.
        """
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                # Use same logic as dashboard: get last 4-5 complete weeks, exclude partial current week
                cursor.execute("""
                    SELECT 
                        date_trunc('week', date) as week_start,
                        SUM(total_applications) as weekly_total,
                        COUNT(*) as total_days
                    FROM daily_progress 
                    WHERE date >= CURRENT_DATE - INTERVAL '6 weeks'
                        AND date < date_trunc('week', CURRENT_DATE)  -- Exclude current partial week
                    GROUP BY date_trunc('week', date)
                    HAVING COUNT(*) = 7  -- Complete weeks only
                    ORDER BY week_start DESC
                    LIMIT 4
                """)
                
                weekly_totals = []
                for row in cursor.fetchall():
                    if row["weekly_total"] > 0:
                        weekly_totals.append(float(row["weekly_total"]))
                
                if weekly_totals and len(weekly_totals) >= 3:
                    # Average of complete weeks (like dashboard)
                    weekly_rate = sum(weekly_totals) / len(weekly_totals)
                    return weekly_rate
                else:
                    return 3000.0  # Default: 3k per week
                
        except Exception as e:
            logger.error(f"Error calculating processing rate: {str(e)}")
            return 3000.0  # Fallback rate
    
    def get_intermediate_backlogs(self, current_month: str, current_year: int, target_month: str, target_year: int) -> List[int]:
        """
        Get backlogs for all months that need to be cleared before target month starts.
        For June to start, we need to clear April + May (not June itself).
        """
        month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                      'July', 'August', 'September', 'October', 'November', 'December']
        
        current_idx = month_order.index(current_month)
        target_idx = month_order.index(target_month)
        
        # Get all months between current and target (exclusive of target)
        months_to_clear = []
        
        if target_year == current_year:
            # Same year: get months from current+1 to target-1 (exclude current, it's already counted)
            for i in range(current_idx + 1, target_idx):
                months_to_clear.append((month_order[i], current_year))
        else:
            # Different year: current+1 to December, then January to target-1
            for i in range(current_idx + 1, 12):
                months_to_clear.append((month_order[i], current_year))
            for i in range(0, target_idx):
                months_to_clear.append((month_order[i], target_year))
        
        # Get backlogs for these months
        backlogs = []
        for month, year in months_to_clear:
            backlog = self.get_month_backlog(month, year)
            if backlog is not None:
                backlogs.append(backlog)
            else:
                backlogs.append(0)  # Default if no data
        
        return backlogs
    
    def get_month_names_between(self, current_month: str, target_month: str) -> List[str]:
        """Get month names between current and target (exclusive of both)."""
        month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                      'July', 'August', 'September', 'October', 'November', 'December']
        
        current_idx = month_order.index(current_month)
        target_idx = month_order.index(target_month)
        
        return [month_order[i] for i in range(current_idx + 1, target_idx)]
    
    def format_timeline(self, weeks_needed: float, estimated_date: 'date') -> str:
        """
        Format timeline in a user-friendly way with precise days for close dates
        and relative descriptions for longer periods.
        """
        from datetime import date
        
        # Calculate precise weeks and days
        total_days = int(weeks_needed * 7)
        weeks = total_days // 7
        extra_days = total_days % 7
        
        # For short timelines (< 3 weeks), show precise days
        if weeks_needed < 3:
            if weeks == 0:
                return f"in {total_days} days"
            elif extra_days == 0:
                week_text = "week" if weeks == 1 else "weeks"
                return f"in {weeks} {week_text}"
            else:
                week_text = "week" if weeks == 1 else "weeks"
                day_text = "day" if extra_days == 1 else "days"
                return f"in {weeks} {week_text} and {extra_days} {day_text}"
        
        # For longer timelines, use relative month descriptions
        today = date.today()
        target_month = estimated_date.strftime('%B')
        target_year = estimated_date.year
        
        # Check if it's early, mid, or late in the month
        if estimated_date.day <= 10:
            month_part = f"beginning of {target_month}"
        elif estimated_date.day <= 20:
            month_part = f"mid-{target_month}"
        else:
            month_part = f"end of {target_month}"
        
        # Add year if different from current year
        if target_year != today.year:
            month_part += f" {target_year}"
        
        # Also include approximate weeks for context
        week_text = "week" if weeks == 1 else "weeks"
        return f"in about {weeks} {week_text}, {month_part}"
    
    def predict_month_start(self, target_month: str, target_year: int) -> Dict[str, Any]:
        """
        Predict when DOL will start processing a specific month based on backlog analysis.
        
        Updated Logic:
        1. Find the current most active month (highest 2024 ANALYST REVIEW backlog)
        2. Get backlogs for current and target months
        3. Calculate total cases to process = current_backlog + target_backlog - 3k threshold
        4. Use weekly processing rate (~3k/week) to predict timeline
        """
        try:
            # Step 1: Find current most active month (2024 with highest ANALYST REVIEW)
            most_active_month = self.get_most_active_month()
            if not most_active_month:
                return {
                    "message": "I couldn't find current processing data to predict month start dates. Please try again later.",
                    "links": []
                }
            
            # Step 2: Get current backlog for most active month  
            current_backlog = self.get_month_backlog(most_active_month["month"], most_active_month["year"])
            if current_backlog is None:
                return {
                    "message": f"I couldn't find backlog data for {most_active_month['month']} {most_active_month['year']}.",
                    "links": []
                }
            
            # Step 3: Get intermediate months' backlogs (months between current and target)
            # We need to clear all months BEFORE target month starts
            intermediate_backlogs = self.get_intermediate_backlogs(
                most_active_month["month"], most_active_month["year"],
                target_month, target_year
            )
            
            # Step 4: Calculate weekly processing rate
            weekly_rate = self.get_average_daily_processing_rate()
            if not weekly_rate or weekly_rate <= 0:
                weekly_rate = 3000.0  # Default 3k per week
            
            # Step 5: Calculate total cases to process before target month starts
            # Need to clear current month backlog AND target month backlog down to ~3k threshold
            target_threshold = 3000
            
            # If we're predicting the next month after current active month
            month_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                          'July', 'August', 'September', 'October', 'November', 'December']
            
            current_month_idx = month_order.index(most_active_month["month"])
            try:
                target_month_idx = month_order.index(target_month)
            except ValueError:
                return {
                    "message": f"Invalid month name: {target_month}",
                    "links": []
                }
            
            # Calculate months between current and target
            if target_year == most_active_month["year"]:
                months_ahead = target_month_idx - current_month_idx
            elif target_year > most_active_month["year"]:
                months_ahead = (12 - current_month_idx) + target_month_idx
            else:
                return {
                    "message": f"{target_month} {target_year} is in the past relative to current processing ({most_active_month['month']} {most_active_month['year']}).",
                    "links": []
                }
            
            if months_ahead <= 0:
                return {
                    "message": f"{target_month} {target_year} is the current or past month. DOL is already processing it or has finished.",
                    "links": []
                }
            
            # Total cases to process: current + all intermediate months down to threshold
            # June starts when April and May are cleared, not when June is cleared
            total_cases_to_process = current_backlog + sum(intermediate_backlogs) - target_threshold
            
            if total_cases_to_process <= 0:
                return {
                    "message": f"Great news! Based on current backlogs, {target_month} {target_year} should start very soon! Current processing is at {most_active_month['month']} {most_active_month['year']} with {current_backlog:,} cases.",
                    "data": {
                        "current_month": most_active_month["month"],
                        "current_year": most_active_month["year"],
                        "current_backlog": current_backlog,
                        "target_month": target_month,
                        "target_year": target_year,
                        "target_backlog": target_backlog
                    },
                    "links": []
                }
            
            weeks_needed = total_cases_to_process / weekly_rate
            
            # Calculate estimated start date with precise days
            from datetime import date, timedelta
            today = date.today()
            total_days = weeks_needed * 7
            estimated_start_date = today + timedelta(days=int(total_days))
            
            # Format timeline with better precision
            timeline_text = self.format_timeline(weeks_needed, estimated_start_date)
            
            # Format the response
            month_name = most_active_month["month"]
            month_year = most_active_month["year"]
            
            # Format intermediate months for display
            intermediate_months = self.get_month_names_between(
                most_active_month["month"], target_month
            )
            intermediate_display = " + ".join([f"{m} ({b:,})" for m, b in zip(intermediate_months, intermediate_backlogs)])
            
            response_message = f"**{target_month} {target_year}** should start around **{estimated_start_date.strftime('%B %d, %Y')}** ({timeline_text})."

            return {
                "message": response_message,
                "data": {
                    "current_month": month_name,
                    "current_year": month_year,
                    "current_backlog": current_backlog,
                    "target_month": target_month,
                    "target_year": target_year,
                    "intermediate_backlogs": intermediate_backlogs,
                    "target_threshold": target_threshold,
                    "weekly_rate": weekly_rate,
                    "total_cases_to_process": total_cases_to_process,
                    "estimated_weeks": int(weeks_needed),
                    "estimated_start_date": estimated_start_date.isoformat(),
                    "months_ahead": months_ahead
                },
                "links": []
            }
            
        except Exception as e:
            logger.error(f"Error in predict_month_start: {str(e)}")
            return {
                "message": f"Sorry, I encountered an error while predicting when {target_month} will start: {str(e)}",
                "links": []
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