import axios from 'axios'

const API_URL = import.meta.env.VITE_API_URL || '/api'

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

export interface Document {
  id: string
  data: Record<string, any>
}

export interface DocumentListResponse {
  documents: Document[]
  total: number
  skip: number
  limit: number
}

export interface FulltextSearchRequest {
  collection: string
  query: string
  limit?: number
  threshold?: number
}

export interface FulltextSearchResult {
  document: Document
  score: number
}

export interface VectorSearchRequest {
  collection: string
  query?: number[]      // 向量查询（可选）
  query_text?: string  // 文本查询（可选，将自动生成 embedding）
  limit?: number
  field?: string
}

export interface VectorSearchResult {
  document: Document
  score: number
}

export const apiClient = {
  // 获取集合列表
  getCollections: async (): Promise<string[]> => {
    const response = await api.get('/db/collections')
    return response.data.collections || []
  },

  // 获取文档列表
  getDocuments: async (
    collection: string,
    skip = 0,
    limit = 100,
    tag?: string
  ): Promise<DocumentListResponse> => {
    const params: Record<string, any> = { skip, limit }
    if (tag) {
      params.tag = tag
    }
    const response = await api.get(`/collections/${collection}/documents`, {
      params,
    })
    return response.data
  },

  // 获取单个文档
  getDocument: async (collection: string, id: string): Promise<Document> => {
    const response = await api.get(`/collections/${collection}/documents/${id}`)
    return response.data
  },

  // 创建文档
  createDocument: async (
    collection: string,
    data: Record<string, any>
  ): Promise<Document> => {
    const response = await api.post(`/collections/${collection}/documents`, data)
    return response.data
  },

  // 更新文档
  updateDocument: async (
    collection: string,
    id: string,
    updates: Record<string, any>
  ): Promise<Document> => {
    const response = await api.put(
      `/collections/${collection}/documents/${id}`,
      updates
    )
    return response.data
  },

  // 删除文档
  deleteDocument: async (collection: string, id: string): Promise<void> => {
    await api.delete(`/collections/${collection}/documents/${id}`)
  },

  // 全文搜索
  fulltextSearch: async (
    collection: string,
    query: string,
    limit = 10,
    threshold = 0
  ): Promise<FulltextSearchResult[]> => {
    const response = await api.post(
      `/collections/${collection}/fulltext/search`,
      {
        collection,
        query,
        limit,
        threshold,
      }
    )
    return response.data.results || []
  },

  // 向量搜索
  vectorSearch: async (
    collection: string,
    query?: number[],
    limit = 10,
    field = 'embedding',
    queryText?: string
  ): Promise<VectorSearchResult[]> => {
    const requestBody: VectorSearchRequest = {
      collection,
      limit,
      field,
    }
    
    if (queryText) {
      requestBody.query_text = queryText
    } else if (query) {
      requestBody.query = query
    } else {
      throw new Error('Either query (vector) or queryText must be provided')
    }
    
    const response = await api.post(
      `/collections/${collection}/vector/search`,
      requestBody
    )
    return response.data.results || []
  },
}

export default api

