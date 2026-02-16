#!/usr/bin/env python3
"""
BTC News & Sentiment Analyzer

Fetches news and analyzes sentiment to generate trading signals.
Sources: Crypto news APIs, Twitter/X sentiment
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass

@dataclass
class SentimentSignal:
    source: str
    sentiment: str  # bullish, bearish, neutral
    confidence: float
    keywords: List[str]
    timestamp: float
    raw_text: str


class NewsSentimentAnalyzer:
    """Analyze news and social media sentiment for BTC."""
    
    def __init__(self):
        self.signals: List[SentimentSignal] = []
        self.keywords_bullish = [
            'surge', 'rally', 'bull', 'breakout', 'adoption', 'institutional',
            'etf', 'approval', 'moon', 'pump', ' ATH', 'all time high'
        ]
        self.keywords_bearish = [
            'crash', 'dump', 'bear', 'regulation', 'ban', 'sec', 'lawsuit',
            'hack', 'fraud', 'scam', 'correction', 'support broken'
        ]
    
    def analyze_text_sentiment(self, text: str) -> Dict:
        """Simple keyword-based sentiment analysis."""
        text_lower = text.lower()
        
        bullish_count = sum(1 for kw in self.keywords_bullish if kw in text_lower)
        bearish_count = sum(1 for kw in self.keywords_bearish if kw in text_lower)
        
        total = bullish_count + bearish_count
        if total == 0:
            return {"sentiment": "neutral", "confidence": 0.5, "score": 0}
        
        score = (bullish_count - bearish_count) / total
        
        if score > 0.3:
            return {"sentiment": "bullish", "confidence": min(abs(score) + 0.5, 0.95), "score": score}
        elif score < -0.3:
            return {"sentiment": "bearish", "confidence": min(abs(score) + 0.5, 0.95), "score": score}
        else:
            return {"sentiment": "neutral", "confidence": 0.5, "score": score}
    
    async def fetch_crypto_news(self, session: aiohttp.ClientSession) -> List[SentimentSignal]:
        """Fetch BTC news from CryptoPanic API (no key required for basic)."""
        signals = []
        
        try:
            # Using CryptoPanic public API
            async with session.get(
                'https://cryptopanic.com/api/v1/posts/',
                params={
                    'auth_token': '',  # Public access (limited)
                    'currencies': 'BTC',
                    'kind': 'news',
                    'limit': '10'
                },
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    for post in data.get('results', []):
                        title = post.get('title', '')
                        sentiment = self.analyze_text_sentiment(title)
                        
                        signal = SentimentSignal(
                            source='CryptoPanic',
                            sentiment=sentiment['sentiment'],
                            confidence=sentiment['confidence'],
                            keywords=[kw for kw in self.keywords_bullish + self.keywords_bearish 
                                     if kw in title.lower()],
                            timestamp=time.time(),
                            raw_text=title[:100]
                        )
                        signals.append(signal)
                        
        except Exception as e:
            print(f"Crypto news fetch error: {e}")
        
        return signals
    
    async def fetch_alternative_news(self, session: aiohttp.ClientSession) -> List[SentimentSignal]:
        """Fetch from alternative.me crypto fear/greed API."""
        signals = []
        
        try:
            async with session.get(
                'https://api.alternative.me/fng/?limit=1',
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    
                    if data.get('data'):
                        item = data['data'][0]
                        value = int(item.get('value', 50))
                        classification = item.get('value_classification', 'neutral')
                        
                        # Convert fear/greed to sentiment
                        if value < 30:  # Extreme fear = bullish (contrarian)
                            sentiment = "bullish"
                            confidence = (30 - value) / 30 * 0.5 + 0.5
                        elif value > 70:  # Extreme greed = bearish (contrarian)
                            sentiment = "bearish"
                            confidence = (value - 70) / 30 * 0.5 + 0.5
                        else:
                            sentiment = "neutral"
                            confidence = 0.5
                        
                        signal = SentimentSignal(
                            source='Fear&Greed',
                            sentiment=sentiment,
                            confidence=confidence,
                            keywords=[classification],
                            timestamp=time.time(),
                            raw_text=f"Fear & Greed Index: {value} ({classification})"
                        )
                        signals.append(signal)
                        
        except Exception as e:
            print(f"Fear/greed fetch error: {e}")
        
        return signals
    
    async def analyze_sentiment(self) -> Dict:
        """Aggregate sentiment from all sources."""
        async with aiohttp.ClientSession() as session:
            # Fetch from all sources
            news_signals = await self.fetch_crypto_news(session)
            feargreed_signals = await self.fetch_alternative_news(session)
            
            all_signals = news_signals + feargreed_signals
            self.signals = all_signals[-20:]  # Keep last 20
            
            if not all_signals:
                return {
                    "overall": "neutral",
                    "confidence": 0.5,
                    "bullish_count": 0,
                    "bearish_count": 0,
                    "neutral_count": 0,
                    "signals": []
                }
            
            # Count sentiments
            bullish = sum(1 for s in all_signals if s.sentiment == 'bullish')
            bearish = sum(1 for s in all_signals if s.sentiment == 'bearish')
            neutral = sum(1 for s in all_signals if s.sentiment == 'neutral')
            
            total = len(all_signals)
            
            # Calculate weighted confidence
            bullish_conf = sum(s.confidence for s in all_signals if s.sentiment == 'bullish')
            bearish_conf = sum(s.confidence for s in all_signals if s.sentiment == 'bearish')
            
            # Determine overall sentiment
            if bullish > bearish and bullish > neutral:
                overall = "bullish"
                confidence = bullish_conf / bullish if bullish > 0 else 0.5
            elif bearish > bullish and bearish > neutral:
                overall = "bearish"
                confidence = bearish_conf / bearish if bearish > 0 else 0.5
            else:
                overall = "neutral"
                confidence = 0.5
            
            return {
                "overall": overall,
                "confidence": min(confidence, 0.95),
                "bullish_count": bullish,
                "bearish_count": bearish,
                "neutral_count": neutral,
                "signals": all_signals
            }
    
    def generate_trading_signal(self) -> Optional[Dict]:
        """Generate trading signal from sentiment."""
        if not self.signals:
            return None
        
        # Count recent signals (last 5 minutes)
        recent_cutoff = time.time() - 300
        recent_signals = [s for s in self.signals if s.timestamp > recent_cutoff]
        
        if len(recent_signals) < 2:
            return None
        
        bullish = sum(1 for s in recent_signals if s.sentiment == 'bullish')
        bearish = sum(1 for s in recent_signals if s.sentiment == 'bearish')
        
        total = bullish + bearish
        if total < 2:
            return None
        
        # Strong consensus needed
        if bullish / total >= 0.7:
            return {
                "strategy": "sentiment",
                "signal": "up",
                "confidence": bullish / total,
                "reason": f"Strong bullish sentiment: {bullish}/{total} signals positive"
            }
        elif bearish / total >= 0.7:
            return {
                "strategy": "sentiment",
                "signal": "down",
                "confidence": bearish / total,
                "reason": f"Strong bearish sentiment: {bearish}/{total} signals negative"
            }
        
        return None


if __name__ == "__main__":
    analyzer = NewsSentimentAnalyzer()
    
    async def test():
        print("Testing sentiment analyzer...")
        result = await analyzer.analyze_sentiment()
        
        print(f"\nOverall Sentiment: {result['overall'].upper()}")
        print(f"Confidence: {result['confidence']:.1%}")
        print(f"Bullish: {result['bullish_count']}, Bearish: {result['bearish_count']}, Neutral: {result['neutral_count']}")
        
        if result['signals']:
            print("\nRecent Signals:")
            for sig in result['signals'][:5]:
                print(f"  [{sig.source}] {sig.sentiment.upper()}: {sig.raw_text[:60]}...")
        
        trade_signal = analyzer.generate_trading_signal()
        if trade_signal:
            print(f"\n🎯 TRADING SIGNAL: {trade_signal['signal'].upper()}")
            print(f"   Confidence: {trade_signal['confidence']:.1%}")
            print(f"   Reason: {trade_signal['reason']}")
    
    asyncio.run(test())
