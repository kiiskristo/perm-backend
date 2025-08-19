from fastapi import APIRouter, Depends, HTTPException
import logging

# Use relative imports if running as a module
try:
    from ...models.database import get_postgres_connection
    from ...models.schemas import ChatbotRequest, ChatbotResponse
    from ...services.chatbot import PermChatbot
except ImportError:
    # Use absolute imports if running as a script
    from src.dol_analytics.models.database import get_postgres_connection
    from src.dol_analytics.models.schemas import ChatbotRequest, ChatbotResponse
    from src.dol_analytics.services.chatbot import PermChatbot

# Set up logging
logger = logging.getLogger("dol_analytics.chatbot")

router = APIRouter(prefix="/chatbot", tags=["chatbot"])


@router.post("/", response_model=ChatbotResponse)
async def chatbot_endpoint(
    request: ChatbotRequest,
    conn=Depends(get_postgres_connection)
):
    """
    Chatbot endpoint that can answer questions about PERM cases.
    
    Example questions:
    - "How many certified cases for April 2024?"
    - "How many N pending for April?" (N = pending cases)
    - "What is my case G-100-24036-692547?"
    - "How long does processing take?"
    - "When will May start?" (month start predictions)
    - "When will June 2024 start being processed?"
    - "Recent cases"
    - "Help" or "What can you do?"
    
    Supported status shortcuts:
    - N = Pending/Analyst Review
    - C = Certified  
    - D = Denied
    - W = Withdrawn
    - R = RFI Issued
    """
    try:
        # Initialize chatbot with database connection
        chatbot = PermChatbot(conn)
        
        # Process the message
        response = chatbot.process_message(request.message)
        
        return ChatbotResponse(**response)
        
    except Exception as e:
        logger.error(f"Chatbot error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing your request: {str(e)}") 