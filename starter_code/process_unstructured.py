import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================

def process_pdf_data(raw_json: dict) -> dict:
    # Bước 1: Làm sạch nhiễu (Header/Footer) khỏi văn bản
    raw_text = raw_json.get("extractedText", "")
    # Xóa các marker dạng HEADER_PAGE_X / FOOTER_PAGE_X rồi chuẩn hóa khoảng trắng
    cleaned_content = re.sub(r"HEADER_PAGE_\d+|FOOTER_PAGE_\d+", "", raw_text)
    cleaned_content = re.sub(r"\s+", " ", cleaned_content).strip()
    
    # Bước 2: Map dữ liệu thô sang định dạng chuẩn của UnifiedDocument
    return {
        "document_id": str(raw_json.get("docId", "")),
        "source_type": "PDF",
        "author": str(raw_json.get("authorName", "Unknown")).strip(),
        "category": str(raw_json.get("docCategory", "Uncategorized")),
        "content": cleaned_content,
        "timestamp": str(raw_json.get("createdAt", "")),
    }

def process_video_data(raw_json: dict) -> dict:
    # Map dữ liệu Video sang định dạng chuẩn, có fallback khi thiếu field
    return {
        "document_id": str(raw_json.get("video_id", "")),
        "source_type": "Video",
        "author": str(raw_json.get("creator_name", "Unknown")).strip(),
        "category": str(raw_json.get("category", "Uncategorized")),
        "content": str(raw_json.get("transcript", "")).strip(),
        "timestamp": str(raw_json.get("published_timestamp", "")),
    }
