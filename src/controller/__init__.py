"""OpenLegion Kubernetes Controller (operator).

Watches AgentFleet and AgentInstance custom resources and manages the
lifecycle of agent pods in response to CR changes.

Usage:
    python -m src.controller

Or with Kopf directly:
    kopf run src/controller/operator.py --namespace openlegion

Prerequisites:
    kubectl apply -f k8s/crds/agentfleet.yaml
    kubectl apply -f k8s/crds/agentinstance.yaml

Optional dependencies (install with: pip install openlegion[k8s]):
    kopf>=1.37.0
    kubernetes>=28.0.0
"""
